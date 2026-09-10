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

from gateway.clients.base import (
    COMMENT_PREFIX,
    EVENT_DATA_PREFIX,
    JSON_CONTENT_TYPE,
    SSE_MEDIA_TYPE,
    BaseClient,
    backend_jsonrpc_error,
)
from gateway.config import BackendConfig
from gateway.errors import BackendDisconnectedError, BackendHttpStatusError, BackendTimeoutError

logger = structlog.get_logger(__name__)

# Intervalo de conexão: falha em conectar não deve esperar o timeout de request
# completo (30s default) para ser reportada.
CONNECT_TIMEOUT_SECONDS = 5.0

class HttpClient(BaseClient):
    """Fala JSON-RPC com um backend MCP remoto via POST na ``url`` do config."""

    def __init__(self, config: BackendConfig, request_timeout: float) -> None:
        super().__init__()
        self._config = config
        self._request_timeout = request_timeout
        self._http: httpx.AsyncClient | None = None
        self._closed = False
        # 3.1 — id único por request (mesmo padrão de SseClient/StdioClient):
        # o id fixo 1 impedia a correlação request→resposta do JSON-RPC.
        self._next_id = 0

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
        self._begin_start()
        self._closed = False
        self._http = httpx.AsyncClient(
            base_url=self._config.url or "",
            headers=self._post_headers(),
            timeout=httpx.Timeout(self._request_timeout, connect=CONNECT_TIMEOUT_SECONDS),
        )
        try:
            await self._initialize()
            self._mark_ready()
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        """Fecha o httpx.AsyncClient de forma limpa (idempotente)."""
        if not self._begin_stop():
            return
        self._closed = True
        try:
            self._fail_pending(
                BackendDisconnectedError(f"backend '{self._config.name}': cliente encerrado")
            )
            http = self._http
            self._http = None
            if http is not None:
                await http.aclose()
        finally:
            self._mark_stopped()

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
        self._next_id += 1
        request_id = self._next_id
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}
        try:
            async with http.stream(
                "POST", "", json=payload, headers=self._post_headers()
            ) as response:
                if response.status_code >= 400:
                    raise BackendHttpStatusError(
                        response.status_code, method, backend=self._config.name
                    )
                content_type = response.headers.get("content-type", "").lower()
                media_type = content_type.split(";", 1)[0].strip()
                # 3.4 — explícito: só application/json e text/event-stream são
                # respostas previstas pelo transporte Streamable HTTP. Qualquer
                # outro Content-Type é resposta inesperada (erro de transporte
                # com o tipo recebido na mensagem), em vez de tentar parsear e
                # só falhar no ValueError.
                if media_type == SSE_MEDIA_TYPE:
                    body = await self._read_sse_response(
                        response.aiter_lines(), request_id, method
                    )
                elif media_type == JSON_CONTENT_TYPE:
                    await response.aread()
                    try:
                        body = response.json()
                    except ValueError as exc:
                        raise BackendDisconnectedError(
                            f"backend '{self._config.name}': resposta não-JSON em '{method}'"
                        ) from exc
                else:
                    raise BackendDisconnectedError(
                        f"backend '{self._config.name}': Content-Type inesperado na resposta"
                        f" de '{method}' ({content_type or 'ausente'})"
                    )
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
        # 3.2 — envelope JSON-RPC: sem o campo jsonrpc (ou com outro valor), o
        # body não é uma resposta válida do protocolo.
        if body.get("jsonrpc") != "2.0":
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': resposta com jsonrpc inválido em '{method}'"
            )
        # 3.3 — correlação por id em AMBOS os caminhos (JSON direto e SSE): o
        # id deixou de ser fixo (3.1), então uma resposta com id divergente é
        # resposta inesperada — não pode ser aceita silenciosamente.
        if body.get("id") != request_id:
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': resposta com id inesperado em '{method}'"
                f" (esperado {request_id}, recebido {body.get('id')!r})"
            )
        error = body.get("error")
        if error is not None:
            raise backend_jsonrpc_error(error)
        # Mesmo contrato de BaseClient._apply_response: resposta sem 'result'
        # e sem 'error' é malformada — nunca devolver None/{} silenciosamente.
        if "result" not in body:
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': resposta malformada em '{method}'"
                " (sem campo 'result' nem 'error')"
            )
        return body["result"]

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
            response = await http.post(
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
            return
        # 3.5 — httpx não levanta para 4xx/5xx: sem esta checagem, um erro HTTP
        # do backend na notificação passaria silenciosamente. Notificação não
        # tem retorno esperado pelo protocolo, então fica registrada em log —
        # não impacta o estado de saúde (o ping do monitor detecta uma queda
        # real logo em seguida).
        if response.status_code >= 400:
            logger.warning(
                "http_notification_falhou",
                backend=self._config.name,
                method=method,
                status_code=response.status_code,
            )

    def _post_headers(self) -> dict[str, str]:
        """Headers do POST JSON-RPC.

        Precedência deliberada (3.6): os headers obrigatórios do transporte
        Streamable HTTP (``JSON_HEADERS``) VENCEM os headers custom do config —
        Content-Type/Accept corretos são parte do contrato do transporte e não
        devem ser sobrescrevíveis por config. Headers custom (ex.: auth do
        backend remoto) são preservados para qualquer outro campo.
        """
        return {**self._config.headers, **JSON_HEADERS}


# Headers obrigatórios do transporte Streamable HTTP. Aplicados POR ÚLTIMO no
# merge de _post_headers: vencem qualquer Content-Type/Accept custom do config
# (decisão deliberada — ver docstring de _post_headers).
JSON_HEADERS = {
    "Content-Type": JSON_CONTENT_TYPE,
    "Accept": f"{JSON_CONTENT_TYPE}, {SSE_MEDIA_TYPE}",
}
