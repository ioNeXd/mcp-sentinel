"""Cliente MCP sobre stdio: sobe o backend como subprocesso e troca JSON-RPC.  
  
Transporte da Fase 0 do ROADMAP. O backend é um processo filho lançado por  
``asyncio.create_subprocess_exec``; a comunicação é JSON-RPC 2.0  
newline-delimited (uma mensagem JSON por linha) sobre stdin/stdout, e o stderr  
do filho é drenado para o log do Gateway.  
  
Duas tasks em background acompanham o processo enquanto ele vive:  
  
- ``_read_stdout``: lê respostas/notificações e as correlaciona aos requests  
  pendentes (via :meth:`BaseClient._apply_response`);  
- ``_read_stderr``: replica o stderr do backend no log estruturado.  
  
A máquina de estados do ciclo de vida, o registro/conclusão de pendências e o  
handshake ``initialize`` são herdados de :class:`BaseClient`; esta classe  
implementa apenas o transporte (subprocesso + streams).  
"""  
  
import asyncio  
import json  
from typing import Any  
  
import structlog  
  
from gateway.clients.base import BaseClient  
from gateway.config import BackendConfig  
from gateway.errors import BackendDisconnectedError, BackendError  
  
logger = structlog.get_logger(__name__)  
  
STOP_GRACE_SECONDS = 3.0  
  
  
class StdioClient(BaseClient):  
    """Client MCP que troca JSON-RPC com um backend rodando como subprocesso.  
  
    O protocolo é newline-delimited: cada mensagem é um objeto JSON numa linha.  
    Respostas são correlacionadas aos requests pelo campo ``id``.  
  
    Atributos de estado próprios do transporte:  
  
    - ``_process``: o subprocesso do backend (``None`` antes de ``start`` e  
      após ``stop``);  
    - ``_reader_task`` / ``_stderr_task``: as tasks de leitura de stdout/stderr;  
    - ``_write_lock``: serializa escritas concorrentes no stdin (uma mensagem  
      nunca se intercala com outra no stream);  
    - ``_closed``: sinaliza encerramento em curso/concluído, para que leituras  
      e escritas tardias falhem cedo em vez de tocar streams já fechados;  
    - ``_reader_failed``: indica que o leitor de stdout morreu por erro  
      inesperado (não pelo término do processo). Nesse caso nenhuma resposta  
      será mais processada, então :meth:`is_alive` passa a devolver ``False``  
      mesmo com o processo tecnicamente vivo — sem isso o backend viraria um  
      "zumbi" que o Health Monitor consideraria saudável.  
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
        self._reader_failed = False  
  
    # ------------------------------------------------------------------  
    # Ciclo de vida  
    # ------------------------------------------------------------------  
  
    async def start(self) -> None:  
        """Sobe o processo, inicia os leitores e faz o handshake ``initialize``.  
  
        Segue o contrato de :class:`BaseClient`: o estado só vira ``ready``  
        depois que ``_initialize`` valida a resposta por completo. Se qualquer  
        etapa falhar (comando ausente/inexistente, handshake malformado ou  
        timeout), ``stop()`` é chamado antes de propagar o erro — nada fica  
        "meio de pé" (processo e tasks órfãos, ou estado inconsistente que faria  
        um ``start`` seguinte achar que o client já está no ar).  
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
        """Encerra o processo e cancela as tasks de leitura (idempotente).  
  
        Encerramento gradual: fecha o stdin, aguarda ``STOP_GRACE_SECONDS`` por  
        um término espontâneo e, se necessário, escala para ``terminate`` e por  
        fim ``kill``. As pendências são falhadas com  
        :class:`BackendDisconnectedError` para que nenhum request fique  
        pendurado até o timeout. Chamadas repetidas/concorrentes são no-op  
        (garantido por ``_begin_stop``).  
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
    # Saúde e envio/recebimento JSON-RPC  
    # ------------------------------------------------------------------  
  
    def is_alive(self) -> bool:  
        """Indica se o backend segue utilizável, sem gerar I/O extra.  
  
        Consulta não bloqueante (``returncode is None``), preferida a um ``ping``  
        com timeout para o Health Monitor: não adiciona tráfego, não exige que o  
        backend implemente ping e detecta o caso mais comum — processo morto —  
        imediatamente. A latência de um backend vivo porém travado é coberta  
        pelo timeout de :meth:`send_request` (que derruba a pendência com  
        ``BackendTimeoutError``).  
  
        Além do processo, considera ``_reader_failed``: se o leitor de stdout  
        morreu por erro inesperado, nenhuma resposta será mais processada e o  
        client é considerado morto ainda que o processo continue vivo.  
        """  
        return (  
            self._process is not None  
            and self._process.returncode is None  
            and not self._reader_failed  
        )  
  
    async def send_request(self, method: str, params: dict[str, Any] | None = None) -> Any:  
        """Envia um request JSON-RPC e aguarda a resposta correlacionada por ``id``.  
  
        Gera um ``id`` monotônico, registra a pendência e delega a espera com  
        timeout a :meth:`BaseClient._await_response`. Qualquer falha ou  
        cancelamento remove a pendência antes de propagar, para não deixar  
        futures órfãs em ``_pending``.  
        """  
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
            return await self._await_response(  
                request_id, future, self._request_timeout, method  
            )  
        except BaseException:  
            self._pop_pending(request_id)  
            raise  
  
    async def _write(self, payload: dict[str, Any]) -> None:  
        """Serializa e escreve um objeto JSON como uma linha no stdin do backend.  
  
        Checa proativamente ``_closed`` e o estado do stdin antes de qualquer  
        I/O: um client encerrado (ou com stdin fechando) falha cedo com  
        :class:`BackendDisconnectedError` em vez de tentar escrever num stream  
        morto. As escritas são serializadas por ``_write_lock`` para não  
        intercalar mensagens.  
        """  
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
        """Escreve uma notificação JSON-RPC (sem ``id``, sem resposta esperada)."""  
        await self._write({"jsonrpc": "2.0", "method": method, "params": params or {}})  
  
    async def _read_stdout(self) -> None:  
        """Lê o stdout do backend linha a linha e despacha cada mensagem.  
  
        Roda como task em background enquanto o processo vive. Um ``readline``  
        vazio significa que o backend fechou o stdout: se isso ocorre fora de um  
        encerramento pedido por nós (``_closed`` falso), marca ``_reader_failed``.  
        Um erro inesperado durante a leitura também marca ``_reader_failed``  
        ANTES de logar, para que :meth:`is_alive` já reflita a falha e o Health  
        Monitor reinicie o backend. Em qualquer desfecho, o ``finally`` derruba  
        as pendências para que nenhum request fique pendurado.  
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
        """Decodifica uma linha do stdout e a encaminha ao pipeline de respostas.  
  
        Linhas que não são JSON válido, mensagens não-objeto e notificações são  
        logadas e ignoradas (não correlacionam a nenhum request). Mensagens com  
        ``id`` e sem ``method`` são candidatas a resposta: se o ``jsonrpc`` não  
        for ``"2.0"`` a mensagem é logada como malformada, e a correlação em si  
        (resolver/rejeitar a pendência) é feita por  
        :meth:`BaseClient._apply_response`.  
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
            if message.get("jsonrpc") != "2.0":  
                logger.warning(  
                    "mensagem malformada sem jsonrpc 2.0 no stdout do backend",  
                    backend=self._config.name,  
                    id=request_id,  
                    jsonrpc=message.get("jsonrpc"),  
                )  
            if not self._apply_response(message):  
                logger.warning(  
                    "resposta inesperada do backend", backend=self._config.name, id=request_id  
                )  
            return  
        logger.debug(  
            "notificação recebida do backend",  
            backend=self._config.name,  
            method=message.get("method"),  
        )  
  
    async def _read_stderr(self) -> None:  
        """Drena o stderr do backend para o log estruturado do Gateway."""  
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