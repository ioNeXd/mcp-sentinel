"""Cliente MCP via stdio: sobe um processo backend e troca JSON-RPC por stdin/stdout.

O transporte é newline-delimited — cada mensagem é um objeto JSON em uma linha.
Respostas são correlacionadas aos requests pelo campo ``id`` (contador único por
client, herdado da BaseClient). Duas tasks de background acompanham o processo:
uma lê o stdout (respostas/notificações) e outra drena o stderr para o log.
"""

import asyncio
import json
from typing import Any

import structlog

from gateway.clients.base import BaseClient
from gateway.config import BackendConfig
from gateway.errors import (
    BackendDisconnectedError,
    BackendError,
)

logger = structlog.get_logger(__name__)

STOP_GRACE_SECONDS = 3.0


class StdioClient(BaseClient):
    """Backend MCP local executado como subprocesso, comunicando via stdio.

    O ciclo de vida (start/stop/handshake) segue o contrato da BaseClient: o
    client só é marcado como pronto após ``initialize`` validar as capabilities.
    """

    def __init__(self, config: BackendConfig, request_timeout: float = 30.0) -> None:
        super().__init__()
        self._config = config
        self._request_timeout = request_timeout
        self._process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._next_id = 0
        self._closed = False
        # Sinaliza que o leitor de stdout morreu por erro inesperado (não pelo
        # término normal do processo). Uma vez setado, o client nunca mais
        # processa respostas e is_alive() passa a reportar False, evitando um
        # backend "zumbi" para o Health Monitor.
        self._reader_failed = False

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Sobe o processo, inicia os leitores e executa o handshake initialize.

        Se o handshake falhar, ``stop()`` é chamado antes de propagar o erro:
        sem isso, o processo e as tasks de leitura ficariam órfãos e um start()
        futuro veria ``self._process`` setado e assumiria que já está de pé.
        """
        self._begin_start()
        self._closed = False
        self._reader_failed = False
        try:
            if self._config.command is None:
                raise BackendError(f"backend '{self._config.name}': comando não configurado")
            try:
                self._process = await asyncio.create_subprocess_exec(
                    self._config.command,
                    *self._config.args,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            except FileNotFoundError as exc:
                raise BackendError(
                    f"backend '{self._config.name}': comando não encontrado: {self._config.command}"
                ) from exc
            self._reader_task = asyncio.create_task(
                self._read_stdout(), name=f"stdio-reader-{self._config.name}"
            )
            self._stderr_task = asyncio.create_task(
                self._read_stderr(), name=f"stdio-stderr-{self._config.name}"
            )
            await self._initialize()
            self._mark_ready()
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        """Encerra o processo (fecha stdin, depois terminate/kill) e cancela as tasks.

        A parada é escalonada: fecha o stdin e aguarda o encerramento gracioso
        por ``STOP_GRACE_SECONDS``; se exceder, ``terminate()`` e nova espera;
        por fim ``kill()``. Idempotente via ``_begin_stop``.
        """
        if not self._begin_stop():
            return
        self._closed = True
        try:
            self._fail_pending(
                BackendDisconnectedError(f"backend '{self._config.name}': cliente encerrado")
            )
            process = self._process
            if process is not None and process.stdin and not process.stdin.is_closing():
                process.stdin.close()
            if process is not None:
                try:
                    await asyncio.wait_for(process.wait(), timeout=STOP_GRACE_SECONDS)
                except asyncio.TimeoutError:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), timeout=STOP_GRACE_SECONDS)
                    except asyncio.TimeoutError:
                        process.kill()
                        await process.wait()
            tasks = [task for task in (self._reader_task, self._stderr_task) if task is not None]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._process = None
            self._reader_task = None
            self._stderr_task = None
        finally:
            self._mark_stopped()

    # ------------------------------------------------------------------
    # Envio/recebimento JSON-RPC
    # ------------------------------------------------------------------

    def is_alive(self) -> bool:
        """Indica se o backend segue utilizável, sem I/O e sem bloquear.

        Consulta ``returncode is None`` (processo vivo) em vez de um ping com
        timeout: não gera tráfego extra, não exige que o backend implemente
        ping, e detecta imediatamente o caso mais comum (processo morto).
        Latência de request (backend vivo mas travado) é coberta pelo timeout
        do próprio ``send_request``. ``_reader_failed`` também invalida o
        client: sem o leitor de stdout, nenhuma resposta seria processada.
        """
        return (
            self._process is not None
            and self._process.returncode is None
            and not self._reader_failed
        )

    async def send_request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Envia um request JSON-RPC e aguarda a resposta correlacionada por id."""
        if self._closed or self._process is None or self._process.returncode is not None:
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': processo não está em execução"
            )
        self._next_id += 1
        request_id = self._next_id
        future = self._register_pending(request_id)
        try:
            await self._write(
                {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}
            )
            return await self._await_response(request_id, future, self._request_timeout, method)
        except BaseException:
            self._pop_pending(request_id)
            raise

    async def _write(self, payload: dict[str, Any]) -> None:
        """Serializa e escreve um objeto JSON-RPC no stdin, serializado pelo lock."""
        if self._closed or self._process is None or self._process.stdin is None:
            raise BackendDisconnectedError(f"backend '{self._config.name}': stdin indisponível")
        if hasattr(self._process.stdin, "is_closing") and self._process.stdin.is_closing():
            raise BackendDisconnectedError(f"backend '{self._config.name}': stdin está fechando")
        async with self._write_lock:
            try:
                self._process.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
                await self._process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError, ProcessLookupError, RuntimeError) as exc:
                raise BackendDisconnectedError(
                    f"backend '{self._config.name}': falha ao escrever no stdin ({exc})"
                ) from exc

    async def _send_notification(self, method: str, params: dict[str, Any] | None = None) -> None:
        """Envia uma notificação JSON-RPC (sem id, sem resposta esperada)."""
        await self._write({"jsonrpc": "2.0", "method": method, "params": params or {}})

    async def _read_stdout(self) -> None:
        """Loop de leitura do stdout: encaminha cada linha para _handle_message.

        Ao fim (EOF, cancelamento ou erro) derruba todas as pendings. Um erro
        inesperado marca ``_reader_failed`` ANTES de logar, para que is_alive()
        já reporte False e o Health Monitor reaja.
        """
        assert self._process is not None and self._process.stdout is not None
        try:
            while True:
                line = await self._process.stdout.readline()
                if not line:
                    if not self._closed:
                        self._reader_failed = True
                    break
                await self._handle_message(line)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._reader_failed = True
            logger.exception("erro inesperado lendo stdout do backend", backend=self._config.name)
        finally:
            self._fail_pending(
                BackendDisconnectedError(
                    f"backend '{self._config.name}': processo encerrou (stdout fechado)"
                )
            )

    async def _handle_message(self, line: bytes) -> None:
        """Decodifica uma linha do stdout e a roteia como resposta ou notificação.

        A validação do envelope de resposta (jsonrpc/result/error) e o log de
        respostas malformadas ou inesperadas são responsabilidade única de
        ``BaseClient._apply_response``; aqui apenas distinguimos uma resposta
        correlacionável (tem ``id`` e não tem ``method``) de uma notificação.
        """
        try:
            message: Any = json.loads(line.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(
                "linha inválida no stdout do backend",
                backend=self._config.name,
                line=line.decode("utf-8", errors="replace"),
            )
            return
        if not isinstance(message, dict):
            logger.warning(
                "mensagem não-dict no stdout do backend",
                backend=self._config.name,
                message=message,
            )
            return
        request_id = message.get("id")
        if request_id is not None and "method" not in message:
            self._apply_response(message)
            return
        logger.debug(
            "notificação recebida do backend",
            backend=self._config.name,
            method=message.get("method"),
        )

    async def _read_stderr(self) -> None:
        """Drena o stderr do processo para o log (uma linha = um aviso)."""
        assert self._process is not None and self._process.stderr is not None
        try:
            async for line in self._process.stderr:
                logger.warning(
                    "stderr do backend",
                    backend=self._config.name,
                    line=line.decode("utf-8", errors="replace").rstrip(),
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("erro lendo stderr do backend", backend=self._config.name)
