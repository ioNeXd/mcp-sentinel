"""Cliente MCP via stdio (Fase 0 do ROADMAP)."""

import asyncio
import json
from typing import Any

import structlog

from gateway.clients.base import BaseClient, set_exception_guarded
from gateway.config import BackendConfig
from gateway.errors import (
    BackendDisconnectedError,
    BackendError,
    BackendJsonRpcError,
    BackendTimeoutError,
)
from gateway.models import INTERNAL_ERROR

logger = structlog.get_logger(__name__)

STOP_GRACE_SECONDS = 3.0


class StdioClient(BaseClient):
    """Sobe um processo backend MCP e troca JSON-RPC via stdin/stdout.

    O protocolo é newline-delimited: cada mensagem é um objeto JSON em uma
    linha. Respostas são correlacionadas com os requests pelo campo ``id``.
    """

    def __init__(self, config: BackendConfig, request_timeout: float = 30.0) -> None:
        self._config = config
        self._request_timeout = request_timeout
        self._process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._pending: dict[int | str, asyncio.Future[Any]] = {}
        self._next_id = 0
        self._closed = False

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Sobe o processo, inicia os leitores e faz o handshake initialize."""
        if self._process is not None:
            return
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

    async def stop(self) -> None:
        """Encerra o processo (stdin fechado, depois terminate/kill) e cancela as tasks."""
        if self._closed:
            return
        self._closed = True
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

    # ------------------------------------------------------------------
    # Envio/recebimento JSON-RPC
    # ------------------------------------------------------------------

    def is_alive(self) -> bool:
        """True enquanto o processo do backend não terminou.

        Consulta não bloqueante (``returncode is None``), escolhida em vez de um
        ``ping`` com timeout para o Health Monitor: não gera tráfego extra no
        backend nem depende de o backend implementar ping, e o caso "processo
        morto" — o mais comum — é detectado imediatamente. Latência de request
        (backend vivo mas travado) é coberta pelo timeout do próprio
        ``send_request`` (que derruba o pending com ``BackendTimeoutError``).
        """
        return self._process is not None and self._process.returncode is None

    async def send_request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Envia um request JSON-RPC e aguarda a resposta correlacionada por id."""
        if self._closed or self._process is None or self._process.returncode is not None:
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': processo não está em execução"
            )
        self._next_id += 1
        request_id = self._next_id
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        await self._write(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}
        )
        try:
            return await asyncio.wait_for(future, timeout=self._request_timeout)
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            raise BackendTimeoutError(
                f"backend '{self._config.name}': sem resposta a '{method}' em {self._request_timeout}s"
            ) from None

    async def _write(self, payload: dict[str, Any]) -> None:
        if self._process is None or self._process.stdin is None:
            raise BackendDisconnectedError(f"backend '{self._config.name}': stdin indisponível")
        async with self._write_lock:
            try:
                self._process.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
                await self._process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError, ProcessLookupError) as exc:
                raise BackendDisconnectedError(
                    f"backend '{self._config.name}': falha ao escrever no stdin ({exc})"
                ) from exc

    async def _send_notification(self, method: str, params: dict[str, Any] | None = None) -> None:
        await self._write({"jsonrpc": "2.0", "method": method, "params": params or {}})

    async def _read_stdout(self) -> None:
        assert self._process is not None and self._process.stdout is not None
        try:
            while True:
                line = await self._process.stdout.readline()
                if not line:
                    break
                await self._handle_message(line)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("erro inesperado lendo stdout do backend", backend=self._config.name)
        finally:
            self._fail_pending(
                BackendDisconnectedError(
                    f"backend '{self._config.name}': processo encerrou (stdout fechado)"
                )
            )

    async def _handle_message(self, line: bytes) -> None:
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
            future = self._pending.pop(request_id, None)
            if future is None or future.done():
                logger.warning(
                    "resposta inesperada do backend", backend=self._config.name, id=request_id
                )
                return
            error = message.get("error")
            if error is not None:
                set_exception_guarded(
                    future,
                    BackendJsonRpcError(
                        code=error.get("code", INTERNAL_ERROR),
                        message=str(error.get("message", "erro do backend")),
                        data=error.get("data"),
                    ),
                    backend=self._config.name,
                )
            else:
                future.set_result(message.get("result"))
            return
        logger.debug(
            "notificação recebida do backend",
            backend=self._config.name,
            method=message.get("method"),
        )

    async def _read_stderr(self) -> None:
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

    def _fail_pending(self, exc: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                # Guarded: idem SseClient — request abandonada não pode virar
                # "Future exception was never retrieved" no GC.
                set_exception_guarded(future, exc, backend=self._config.name)
        self._pending.clear()