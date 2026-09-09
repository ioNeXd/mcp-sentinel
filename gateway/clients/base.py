"""Interface comum dos clients de backend MCP (stdio, http e sse)."""

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import structlog

from gateway.errors import BackendError, BackendJsonRpcError
from gateway import __version__
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

    Requests podem ser abandonadas sem que a future saia de ``_pending`` no
    mesmo tick (ex.: um ``wait_for`` externo do health check cancela o
    ``send_request`` enquanto a resposta está a caminho). Se uma future órfã
    recebe ``set_exception`` e ninguém a recupera, o GC dispara o aviso
    "Future exception was never retrieved" pelo handler do asyncio — fora do
    structlog. O callback abaixo recupera a exceção (marcando-a como lida,
    o que suprime o aviso) e, em debug, registra o descarte via structlog.

    Esta função é segura por si só: se a future já está done (incluindo
    cancelada), retorna imediatamente sem chamar ``set_exception`` — o que
    evitava ``asyncio.InvalidStateError`` caso um chamador invocasse sem
    checar o estado da future previamente.
    """
    if future.done():
        return

    future.set_exception(exc)

    def _consume(done: asyncio.Future[Any]) -> None:
        if done.cancelled():
            return
        error = done.exception()  # marca como recuperada: suprime o aviso do asyncio
        if error is not None:
            logger.debug(
                "future_exception_descartada", backend=backend, error=str(error)
            )

    future.add_done_callback(_consume)


class BaseClient(ABC):
    """Contrato de um client MCP: conecta a um backend e troca mensagens JSON-RPC.

    Implementações: :class:`gateway.clients.stdio_client.StdioClient` (Fase 0),
    :class:`gateway.clients.http_client.HttpClient` e
    :class:`gateway.clients.sse_client.SseClient` (Fase 3).

    Estados compartilhados entre transportes:

    - ``_pending``: futures de requests no aguardo de resposta, indexadas pelo
      id JSON-RPC (clients com canal de leitura em background: stdio e sse);
    - ``_capabilities``: capabilities anunciadas pelo backend no handshake
      (inicializado aqui para NÃO ser um atributo mutável de classe — um dict
      de classe seria compartilhado entre todas as instâncias até o primeiro
      set do setter).
    """

    def __init__(self) -> None:
        self._pending: dict[int | str, asyncio.Future[Any]] = {}
        self._capabilities: dict[str, Any] = {}

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
        primeira verificação de saúde. A implementação padrão assume vivo
        (clients sem processo próprio, como o FakeClient dos testes, herdam
        isso); clients com processo substituem o comportamento.
        """
        return True

    @abstractmethod
    async def send_request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Envia um request JSON-RPC e devolve o campo 'result' da resposta.

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

        Compartilhado por todos os transportes: o protocolo é o mesmo, só o
        meio de envio muda. As capabilities anunciadas pelo backend ficam
        guardadas em :attr:`capabilities`; implementações sobrescrevem
        ``_send_notification`` quando sua notificação tem forma particular.
        """
        result = await self.send_request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "mcp-gateway", "version": __version__},
            },
        )
        # 1.3 — validar resposta de initialize do backend
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
            raise BackendError(
                "Backend respondeu capabilities inválidas no initialize"
            )
        self.capabilities = backend_caps
        await self._send_notification("notifications/initialized")

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
        que nenhum request fique pendurado até o timeout. Cada future recebe
        a exceção via :func:`set_exception_guarded` — a request pode ter sido
        abandonada (ex.: ``wait_for`` externo desistiu) e ninguém vai recuperar
        a exceção; o callback interno a consome e suprime o aviso do asyncio.
        """
        for future in self._pending.values():
            if not future.done():
                set_exception_guarded(future, exc, backend=self._backend_name())
        self._pending.clear()

    def _backend_name(self) -> str:
        """Nome do backend para logs (cada client guarda o próprio config)."""
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
        respondeu algo malformado". Resposta estruturalmente inválida
        (não-dict, campo ausente ou valor não-lista)
        é um erro de domínio — não vira ``[]`` silenciosamente.
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
