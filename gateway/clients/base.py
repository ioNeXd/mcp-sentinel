"""Interface comum dos clients de backend MCP (stdio, http e sse)."""  
  
import asyncio  
from abc import ABC, abstractmethod  
from typing import Any  
  
import structlog  
  
from gateway import __version__  
from gateway.errors import (  
    BackendError,  
    BackendJsonRpcError,  
    BackendStateConflictError,  
    BackendTimeoutError,  
)  
from gateway.models import (  
    INTERNAL_ERROR,  
    METHOD_NOT_FOUND,  
    PROTOCOL_VERSION,  
    is_supported_protocol_version,  
)  
  
JSON_CONTENT_TYPE = "application/json"  
SSE_MEDIA_TYPE = "text/event-stream"  
EVENT_DATA_PREFIX = "data:"  
COMMENT_PREFIX = ":"  
  
# Estados do ciclo de vida compartilhado entre transportes (ver BaseClient).  
LIFECYCLE_NEW = "new"  # criado; conexão ainda não estabelecida  
LIFECYCLE_STARTING = "starting"  # conexão em curso; handshake não validado  
LIFECYCLE_READY = "ready"  # handshake validado; pronto para requests  
LIFECYCLE_STOPPING = "stopping"  # encerramento em curso  
LIFECYCLE_STOPPED = "stopped"  # encerrado; recursos liberados  
  
logger = structlog.get_logger(__name__)  
  
  
class BackendListResponseError(BackendError):  
    """Resposta estruturalmente inválida de uma operação de listagem."""  
  
  
def backend_jsonrpc_error(error: Any) -> BackendJsonRpcError:  
    """Converte o campo ``error`` de uma resposta JSON-RPC em erro de domínio."""  
    if isinstance(error, dict):  
        return BackendJsonRpcError(  
            code=error.get("code", INTERNAL_ERROR),  
            message=str(error.get("message", "erro do backend")),  
            data=error.get("data"),  
        )  
    return BackendJsonRpcError(code=INTERNAL_ERROR, message=str(error))  
  
  
def set_exception_guarded(future: asyncio.Future[Any], exc: Exception, *, backend: str) -> None:  
    """Define a exceção numa future garantindo que ela seja sempre consumida.  
  
    Uma request pode ser abandonada sem que a future saia de ``_pending`` no  
    mesmo tick (ex.: um ``wait_for`` externo do health check cancela o  
    ``send_request`` enquanto a resposta ainda está a caminho). Se uma future  
    órfã recebe ``set_exception`` e ninguém a recupera, o GC dispara o aviso  
    "Future exception was never retrieved" pelo handler do asyncio — fora do  
    structlog. O callback interno recupera a exceção (suprimindo o aviso) e,  
    em debug, registra o descarte.  
  
    É segura por si só: se a future já está concluída (inclusive cancelada),  
    retorna sem chamar ``set_exception`` — evitando ``InvalidStateError``.  
    """  
    if future.done():  
        return  
  
    future.set_exception(exc)  
  
    def _consume(done: asyncio.Future[Any]) -> None:  
        if done.cancelled():  
            return  
        error = done.exception()  # marca como recuperada: suprime o aviso do asyncio  
        if error is not None:  
            logger.debug("future_exception_descartada", backend=backend, error=str(error))  
  
    future.add_done_callback(_consume)  
  
  
class BaseClient(ABC):  
    """Contrato de um client MCP: conecta a um backend e troca mensagens JSON-RPC.  
  
    Implementações: :class:`gateway.clients.stdio_client.StdioClient` (Fase 0),  
    :class:`gateway.clients.http_client.HttpClient` e  
    :class:`gateway.clients.sse_client.SseClient` (Fase 3).  
  
    Estados compartilhados entre transportes:  
  
    - ``_pending``: futures de requests aguardando resposta, indexadas pelo id  
      JSON-RPC (clients com leitura em background: stdio e sse);  
    - ``_capabilities``: capabilities anunciadas pelo backend no handshake  
      (inicializado por instância para NÃO virar atributo mutável de classe,  
      que seria compartilhado entre todas as instâncias até o primeiro set).  
  
    Ciclo de vida orquestrado pelos clients concretos dentro de start/stop,  
    para que todos os transportes tenham o MESMO comportamento:  
  
    - ``start()``: ``_begin_start()`` -> conectar -> ``_initialize()`` (valida  
      tudo) -> ``_mark_ready()``; qualquer falha faz ``stop()`` e re-raise  
      (nada fica "meio de pé" — nem capabilities publicadas, nem estado ready);  
    - ``stop()``: ``_begin_stop()`` (idempotente: False se já parando/parado)  
      -> ``_fail_pending(disconnected)`` -> liberar recursos -> ``_mark_stopped()``.  
  
    Conclusão de pendentes: nenhum transporte conclui uma future diretamente —  
    toda conclusão passa por ``_resolve_pending``/``_reject_pending`` (pop +  
    checagem de ``done()``), de modo que uma request NUNCA é concluída duas  
    vezes (resposta tardia + timeout, ou abandono por cancelamento seguido da  
    chegada da resposta).  
    """  
  
    def __init__(self) -> None:  
        self._pending: dict[int | str, asyncio.Future[Any]] = {}  
        self._capabilities: dict[str, Any] = {}  
        self._state: str = LIFECYCLE_NEW  
        self._initializing = False  
  
    @property  
    def state(self) -> str:  
        """Estado atual do ciclo de vida (ver constantes ``LIFECYCLE_*``)."""  
        return self._state  
  
    @property  
    def is_ready(self) -> bool:  
        """True só quando o handshake foi validado por completo."""  
        return self._state == LIFECYCLE_READY  
  
    def _begin_start(self) -> None:  
        """Entra em ``starting``; levanta conflito se já iniciado/em curso.  
  
        Guarda de idempotência e de corrida: um segundo ``start()`` concorrente  
        (ou um restart durante ``stopping``) não pode duplicar conexão nem  
        handshake.  
        """  
        backend = self._backend_name()  
        if self._state == LIFECYCLE_READY:  
            raise BackendStateConflictError(f"{backend}: já iniciado")  
        if self._state == LIFECYCLE_STARTING:  
            raise BackendStateConflictError(f"{backend}: start já em curso")  
        if self._state == LIFECYCLE_STOPPING:  
            raise BackendStateConflictError(f"{backend}: start incompatível com stop em curso")  
        self._state = LIFECYCLE_STARTING  
  
    def _mark_ready(self) -> None:  
        """Publica o estado ``ready`` — só após ``_initialize`` validar tudo."""  
        if self._state != LIFECYCLE_STARTING:  
            raise BackendStateConflictError(  
                f"{self._backend_name()}: _mark_ready chamado em estado '{self._state}'"  
            )  
        self._state = LIFECYCLE_READY  
  
    def _begin_stop(self) -> bool:  
        """Entra em ``stopping``. Devolve True só para o primeiro chamador.  
  
        Idempotência do ``stop()``: chamadas repetidas ou concorrentes devolvem  
        False e NÃO re-executam a liberação de recursos (responsabilidade do  
        primeiro chamador).  
        """  
        if self._state in (LIFECYCLE_STOPPING, LIFECYCLE_STOPPED):  
            return False  
        self._state = LIFECYCLE_STOPPING  
        return True  
  
    def _mark_stopped(self) -> None:  
        """Conclui o encerramento. Requests passam a falhar com conflito de estado."""  
        self._state = LIFECYCLE_STOPPED  
  
    def _ensure_ready(self, action: str) -> None:  
        """Guarda usada antes de enviar requests: exige estado ``ready``."""  
        if self._state != LIFECYCLE_READY:  
            raise BackendStateConflictError(  
                f"{self._backend_name()}: '{action}' requer client pronto "  
                f"(estado atual: '{self._state}')"  
            )  
  
    def _register_pending(self, request_id: int | str) -> asyncio.Future[Any]:  
        """Cria e registra a future de resposta para um id JSON-RPC.  
  
        Levanta :class:`BackendStateConflictError` se o id já está pendente —  
        ids duplicados indicariam corrida de correlação inaceitável.  
        """  
        if request_id in self._pending:  
            raise BackendStateConflictError(  
                f"{self._backend_name()}: id {request_id} já pendente"  
            )  
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()  
        self._pending[request_id] = future  
        return future  
  
    def _pop_pending(self, request_id: int | str) -> asyncio.Future[Any] | None:  
        """Remove (sem concluir) a future do id; None se inexistente."""  
        return self._pending.pop(request_id, None)  
  
    def _resolve_pending(self, request_id: int | str, result: Any) -> bool:  
        """Conclui uma pending com sucesso. False se ela não existe mais.  
  
        Uma request abandonada (timeout, cancelamento, desconexão) já saiu de  
        ``_pending``: resposta tardia devolve False e é descartada pelo caller —  
        nunca concluída duas vezes.  
        """  
        future = self._pending.pop(request_id, None)  
        if future is None or future.done():  
            return False  
        future.set_result(result)  
        return True  
  
    def _reject_pending(self, request_id: int | str, exc: Exception) -> bool:  
        """Falha uma pending com erro. False se ela não existe mais (mesmo contrato)."""  
        future = self._pending.pop(request_id, None)  
        if future is None or future.done():  
            return False  
        set_exception_guarded(future, exc, backend=self._backend_name())  
        return True  
  
    async def _await_response(  
        self,  
        request_id: int | str,  
        future: asyncio.Future[Any],  
        timeout: float,  
        method: str,  
    ) -> Any:  
        """Aguarda a resposta correlacionada a um request já registrado.  
  
        Contrato compartilhado de timeout/cancelamento:  
  
        - ``asyncio.TimeoutError``: a pending é REMOVIDA e o caller recebe  
          :class:`BackendTimeoutError` (nunca o timeout cru) — a resposta  
          tardia será tratada como inesperada, sem conclusão dupla;  
        - ``asyncio.CancelledError``: a pending também é removida e a exceção é  
          SEMPRE re-propagada — cancelamento nunca é engolido nem convertido  
          em resultado vazio ou erro de domínio.  
        """  
        try:  
            return await asyncio.wait_for(future, timeout)  
        except asyncio.TimeoutError:  
            self._pop_pending(request_id)  
            raise BackendTimeoutError(  
                f"{self._backend_name()}: sem resposta para '{method}' em {timeout}s"  
            ) from None  
        except asyncio.CancelledError:  
            self._pop_pending(request_id)  
            raise  
  
    @abstractmethod  
    async def start(self) -> None:  
        """Conecta ao backend e conclui o handshake (initialize)."""  
        raise NotImplementedError  
  
    @abstractmethod  
    async def stop(self) -> None:  
        """Encerra a conexão/processo e libera recursos."""  
        raise NotImplementedError  
  
    def is_alive(self) -> bool:  
        """Indica se o transporte com o backend segue utilizável.  
  
        Consulta barata e sem I/O, usada pelo Health Monitor (Fase 2) como  
        primeira verificação de saúde. O default assume vivo (clients sem  
        processo próprio, como o FakeClient dos testes, herdam isso); clients  
        com processo/canal de leitura substituem o comportamento.  
        """  
        return True  
  
    @abstractmethod  
    async def send_request(self, method: str, params: dict[str, Any] | None = None) -> Any:  
        """Envia um request JSON-RPC e devolve o campo ``result`` da resposta.  
  
        Levanta :class:`gateway.errors.BackendError` (ou subclasses) em caso de  
        timeout, desconexão ou erro JSON-RPC do backend.  
        """  
        raise NotImplementedError  
  
    async def _send_notification(self, method: str, params: dict[str, Any] | None = None) -> None:  
        """Envia uma notificação JSON-RPC (sem id, sem resposta esperada).  
  
        Transportes em que notificação é um request comum (HTTP) sobrescrevem;  
        nos demais, o default é no-op (stdio já escreve direto no stream dele).  
        """  
        return None  
  
    async def _initialize(self) -> None:  
        """Handshake MCP: initialize + notifications/initialized.  
  
        Compartilhado por todos os transportes. Contrato:  
  
        - só TERMINA após a validação completa da resposta (objeto JSON,  
          ``protocolVersion`` suportado e igual ao do Gateway, ``capabilities``  
          dict) — qualquer falha levanta ``BackendError``;  
        - ``capabilities`` só é publicado DEPOIS de toda a validação, e a  
          notificação ``notifications/initialized`` só é enviada em seguida:  
          um handshake malformado nunca vira "meio inicializado";  
        - não é reentrante: chamada concorrente levanta conflito de estado (o  
          start é responsabilidade de um único caller).  
        """  
        if self._initializing:  
            raise BackendStateConflictError(f"{self._backend_name()}: initialize já em curso")  
        self._initializing = True  
        try:  
            result = await self.send_request(  
                "initialize",  
                {  
                    "protocolVersion": PROTOCOL_VERSION,  
                    "capabilities": {},  
                    "clientInfo": {"name": "mcp-gateway", "version": __version__},  
                },  
            )  
            if not isinstance(result, dict):  
                raise BackendError("Backend retornou resultado de initialize não-objeto")  
            backend_protocol = result.get("protocolVersion")  
            backend_caps = result.get("capabilities")  
            if (  
                not isinstance(backend_protocol, str)  
                or not is_supported_protocol_version(backend_protocol)  
                or backend_protocol != PROTOCOL_VERSION  
            ):  
                raise BackendError(  
                    "Backend respondeu uma versão de protocolo incompatível no initialize"  
                )  
            if backend_caps is None or not isinstance(backend_caps, dict):  
                raise BackendError("Backend respondeu capabilities inválidas no initialize")  
            self.capabilities = backend_caps  
            await self._send_notification("notifications/initialized")  
        finally:  
            self._initializing = False  
  
    def _apply_response(self, message: Any) -> bool:  
        """Correlaciona e aplica uma mensagem de RESPOSTA do backend.  
  
        Usado pelos leitores em background (stdio, sse): o leitor NÃO faz pop  
        nem conclui futures diretamente; a conclusão acontece aqui.  
  
        Contrato de respostas malformadas: uma mensagem com ``id`` de request  
        que NÃO seja uma resposta válida (``jsonrpc != "2.0"``, ou sem  
        ``result`` E sem ``error``) FALHA a pending com ``BackendError`` —  
        nunca resolve com resultado vazio (``None``/``{}``). Devolve True se a  
        mensagem foi aplicada a alguma pending (ou descartada por tardia/  
        inesperada); False se não é uma resposta (ex.: notificação) e o caller  
        deve logar.  
        """  
        backend = self._backend_name()  
        if not isinstance(message, dict):  
            logger.warning("resposta não-objeto do backend", backend=backend, message=message)  
            return False  
        request_id = message.get("id")  
        if request_id is None or "method" in message:  
            return False  
        if message.get("jsonrpc") != "2.0":  
            self._reject_pending(  
                request_id,  
                BackendError(  
                    f"resposta malformada: jsonrpc '{message.get('jsonrpc')}' != '2.0'"  
                ),  
            )  
            return True  
        error = message.get("error")  
        if error is not None:  
            self._reject_pending(request_id, backend_jsonrpc_error(error))  
            return True  
        if "result" not in message:  
            self._reject_pending(  
                request_id, BackendError("resposta malformada: sem campo 'result' nem 'error'")  
            )  
            return True  
        resolved = self._resolve_pending(request_id, message["result"])  
        if not resolved:  
            logger.warning(  
                "resposta inesperada do backend (request abandonada ou id desconhecido)",  
                backend=backend,  
                id=request_id,  
            )  
        return resolved  
  
    @property  
    def capabilities(self) -> dict[str, Any]:  
        """Capabilities anunciadas pelo backend no handshake (vazio antes dele)."""  
        return self._capabilities  
  
    @capabilities.setter  
    def capabilities(self, value: dict[str, Any]) -> None:  
        self._capabilities = value  
  
    def _fail_pending(self, exc: Exception) -> None:  
        """Falha todos os requests pendentes e limpa ``_pending``.  
  
        Compartilhado pelos clients com leitura em background (stdio e sse):  
        chamado quando o canal de resposta cai ou o client é encerrado, para  
        que nenhum request fique pendurado até o timeout. Cada future recebe a  
        exceção via :func:`set_exception_guarded` — a request pode ter sido  
        abandonada e ninguém vai recuperar a exceção; o callback interno a  
        consome e suprime o aviso do asyncio.  
        """  
        for future in self._pending.values():  
            if not future.done():  
                set_exception_guarded(future, exc, backend=self._backend_name())  
        self._pending.clear()  
  
    def _backend_name(self) -> str:  
        """Nome exibido em logs/erros; subclasses devem sobrescrever com o config.  
  
        AVISO: o default retorna o nome da CLASSE — subclasses (stdio/http/sse)  
        deveriam sobrescrever para devolver ``self._config.name``. Ver bug  
        registrado.  
        """  
        return type(self).__name__  
  
    async def list_tools(self) -> list[dict[str, Any]]:  
        """Pede ``tools/list`` ao backend e devolve a lista de tools."""  
        result = await self.send_request("tools/list")  
        return self._extract_list(result, "tools")  
  
    async def list_resources(self) -> list[dict[str, Any]]:  
        """Pede ``resources/list`` ao backend e devolve a lista de resources.  
  
        Nem todo backend MCP implementa resources: se o backend responder  
        MethodNotFound (-32601), trata como lista vazia em vez de erro — o  
        Gateway agrega o que existir e não exige suporte de todos.  
        """  
        return await self._list_graceful("resources/list", "resources")  
  
    async def list_prompts(self) -> list[dict[str, Any]]:  
        """Pede ``prompts/list`` ao backend e devolve a lista de prompts.  
  
        Mesmo tratamento gracioso de :meth:`list_resources`: backend sem  
        suporte a prompts vira lista vazia, não erro.  
        """  
        return await self._list_graceful("prompts/list", "prompts")  
  
    async def _list_graceful(self, method: str, key: str) -> list[dict[str, Any]]:  
        try:  
            result = await self.send_request(method)  
        except BackendJsonRpcError as exc:  
            if exc.code == METHOD_NOT_FOUND:  
                return []  
            raise  
        return self._extract_list(result, key)  
  
    @staticmethod  
    def _extract_list(result: Any, key: str) -> list[dict[str, Any]]:  
        """Extrai itens de uma resposta de listagem.  
  
        Diferencia "backend respondeu lista vazia válida" de "backend  
        respondeu algo malformado": resposta estruturalmente inválida  
        (não-dict, campo ausente ou valor não-lista) é erro de domínio —  
        não vira ``[]`` silenciosamente.  
        """  
        if not isinstance(result, dict):  
            raise BackendListResponseError(  
                f"Resposta de listagem não é objeto JSON: {type(result).__name__}"  
            )  
        if key not in result:  
            raise BackendListResponseError(  
                f"Resposta de listagem não contém campo '{key}'"  
            )  
        items = result[key]  
        if not isinstance(items, list):  
            raise BackendListResponseError(  
                f"Campo '{key}' não é uma lista: {type(items).__name__}"  
            )  
        return items