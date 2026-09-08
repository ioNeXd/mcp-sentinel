"""Interface comum dos clients de backend MCP (stdio, http e sse)."""

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import structlog

from gateway.errors import BackendJsonRpcError
from gateway import __version__
from gateway.models import INTERNAL_ERROR, METHOD_NOT_FOUND, PROTOCOL_VERSION

JSON_CONTENT_TYPE = "application/json"
SSE_MEDIA_TYPE = "text/event-stream"
EVENT_DATA_PREFIX = "data:"
COMMENT_PREFIX = ":"

logger = structlog.get_logger(__name__)


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
    """
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
    """

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
        if isinstance(result, dict):
            self.capabilities = result.get("capabilities", {})
        await self._send_notification("notifications/initialized")

    @property
    def capabilities(self) -> dict[str, Any]:
        """Capabilities anunciadas pelo backend no handshake (vazio antes dele)."""
        return self._capabilities

    @capabilities.setter
    def capabilities(self, value: dict[str, Any]) -> None:
        self._capabilities = value

    _capabilities: dict[str, Any] = {}

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
        if not isinstance(result, dict):
            return []
        items = result.get(key, [])
        return items if isinstance(items, list) else []
