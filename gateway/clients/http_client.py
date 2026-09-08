"""HttpClient: backend MCP remoto via JSON-RPC sobre POST (Fase 3).

Transporte sem estado: cada request JSON-RPC é um POST com o envelope completo
e a resposta chega no corpo HTTP — não há correlação por id (o backend HTTP
responde na mesma transação). ``initialize``/``ping`` são requests comuns.

Timeouts e erros de rede são convertidos nas exceções de domínio do Gateway
(``BackendTimeoutError``/``BackendDisconnectedError``) — as mesmas que o
``StdioClient`` levanta para processo travado/morto — de modo que o restante
do Gateway (McpServer, Health Monitor, auto-restart) nem precisa saber qual
transporte está por trás.
"""

import json
from typing import Any
from collections.abc import AsyncIterator

import httpx
import structlog

from gateway.clients.base import PROTOCOL_VERSION, BaseClient
from gateway.config import BackendConfig
from gateway.errors import BackendDisconnectedError, BackendJsonRpcError, BackendTimeoutError
from gateway.models import INTERNAL_ERROR

logger = structlog.get_logger(__name__)

# Intervalo de conexão: falha em conectar não deve esperar o timeout de request
# completo (30s default) para ser reportada.
CONNECT_TIMEOUT_SECONDS = 5.0

JSON_CONTENT_TYPE = "application/json"
SSE_MEDIA_TYPE = "text/event-stream"
EVENT_DATA_PREFIX = "data:"
COMMENT_PREFIX = ":"


class HttpClient(BaseClient):
    """Fala JSON-RPC com um backend MCP remoto via POST na ``url`` do config."""

    def __init__(self, config: BackendConfig, request_timeout: float) -> None:
        self._config = config
        self._request_timeout = request_timeout
        self._http: httpx.AsyncClient | None = None
        self._closed = False

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Cria o client HTTP e valida conectividade com o handshake initialize.

        Não sobe processo nenhum: o backend já deve estar no ar na ``url``
        configurada (o Gateway não inicia backends remotos — decisão do
        ROADMAP: o config é explícito). ``initialize`` serve de health check
        inicial: se o backend não responder, o start falha e o auto-restart
        da Fase 2 cuida de tentar de novo com backoff.
        """
        if self._http is not None:
            return
        self._http = httpx.AsyncClient(
            base_url=self._config.url or "",
            headers=self._post_headers(),
            timeout=httpx.Timeout(self._request_timeout, connect=CONNECT_TIMEOUT_SECONDS),
        )
        try:
            await self._initialize()
        except BaseException:
            await self._http.aclose()
            self._http = None
            raise

    async def stop(self) -> None:
        """Fecha o httpx.AsyncClient de forma limpa (idempotente)."""
        if self._closed:
            return
        self._closed = True
        http = self._http
        self._http = None
        if http is not None:
            await http.aclose()

    def is_alive(self) -> bool:
        """Vivo enquanto o client HTTP não foi fechado (sem I/O aqui).

        A checagem de rede de verdade fica no ping do Health Monitor —
        para HTTP não há ``returncode`` barato como no stdio.
        """
        return self._http is not None and not self._closed

    # ------------------------------------------------------------------
    # JSON-RPC sobre POST
    # ------------------------------------------------------------------

    async def send_request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """POST do envelope JSON-RPC; devolve o campo ``result`` da resposta."""
        http = self._http
        if self._closed or http is None:
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': cliente HTTP fechado"
            )
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
        try:
            request_id = 1
            async with http.stream("POST", "", json=payload, headers=self._post_headers()) as response:
                if response.status_code >= 400:
                    raise BackendDisconnectedError(
                        f"backend '{self._config.name}': HTTP {response.status_code} em '{method}'"
                    )
                content_type = response.headers.get("content-type", "").lower()
                if SSE_MEDIA_TYPE in content_type:
                    body = await self._read_sse_response(
                        response.aiter_lines(), request_id, method
                    )
                else:
                    await response.aread()
                    try:
                        body = response.json()
                    except ValueError as exc:
                        raise BackendDisconnectedError(
                            f"backend '{self._config.name}': resposta não-JSON em '{method}'"
                        ) from exc
        except httpx.TimeoutException as exc:
            raise BackendTimeoutError(
                f"backend '{self._config.name}': sem resposta a '{method}'"
                f" em {self._request_timeout}s"
            ) from exc
        except httpx.HTTPError as exc:
            # Erro de conexão/rede = o equivalente HTTP de "processo morto".
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': falha de rede ({exc.__class__.__name__})"
            ) from exc

        if not isinstance(body, dict):
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': resposta JSON-RPC malformada em '{method}'"
            )
        error = body.get("error")
        if error is not None:
            raise BackendJsonRpcError(
                code=error.get("code", INTERNAL_ERROR) if isinstance(error, dict) else INTERNAL_ERROR,
                message=str(error.get("message", "erro do backend"))
                if isinstance(error, dict)
                else str(error),
                data=error.get("data") if isinstance(error, dict) else None,
            )
        return body.get("result")

    async def _read_sse_response(
        self, lines: AsyncIterator[str], request_id: int, method: str
    ) -> dict[str, Any]:
        """Lê eventos SSE até a resposta JSON-RPC desta requisição."""
        data_lines: list[str] = []

        async for line in lines:
            if line.strip() == "":
                if data_lines:
                    body = self._parse_sse_json("\n".join(data_lines), method)
                    if body.get("id") == request_id:
                        return body
                    data_lines = []
                continue
            if line.startswith(EVENT_DATA_PREFIX):
                data_lines.append(line[len(EVENT_DATA_PREFIX) :].lstrip())
            elif line.startswith(COMMENT_PREFIX):
                continue

        raise BackendDisconnectedError(
            f"backend '{self._config.name}': stream SSE terminou sem resposta a '{method}'"
        )

    def _parse_sse_json(self, data: str, method: str) -> dict[str, Any]:
        try:
            body: Any = json.loads(data)
        except ValueError as exc:
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': evento SSE não-JSON em '{method}'"
            ) from exc
        if not isinstance(body, dict):
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': evento SSE malformado em '{method}'"
            )
        return body

    async def _send_notification(self, method: str, params: dict[str, Any] | None = None) -> None:
        """HTTP não tem canal de notificação: notifications/initialized vira POST."""
        http = self._http
        if self._closed or http is None:
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': cliente HTTP fechado"
            )
        try:
            await http.post(
                "",
                json={"jsonrpc": "2.0", "method": method, "params": params or {}},
                headers=self._post_headers(),
            )
        except httpx.HTTPError:
            # Notificação não tem resposta esperada; falha de rede aqui é logada
            # e ignorada (o health check do monitor detectaria logo em seguida).
            logger.warning(
                "http_notification_falhou", backend=self._config.name, method=method
            )

    async def _initialize(self) -> None:
        """Handshake MCP + guarda as capabilities anunciadas pelo backend."""
        result = await self.send_request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "mcp-gateway", "version": "0.1.0"},
            },
        )
        if isinstance(result, dict):
            self.capabilities = result.get("capabilities", {})
        await self._send_notification("notifications/initialized")

    def _post_headers(self) -> dict[str, str]:
        """Headers obrigatórios do Streamable HTTP em cada POST."""
        return {**self._config.headers, **JSON_HEADERS}


# Headers exigidos pelo transporte Streamable HTTP; headers customizados do
# config continuam sendo mesclados por cima.
JSON_HEADERS = {
    "Content-Type": JSON_CONTENT_TYPE,
    "Accept": f"{JSON_CONTENT_TYPE}, {SSE_MEDIA_TYPE}",
}