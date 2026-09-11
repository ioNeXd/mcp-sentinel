"""SseClient: backend MCP remoto via SSE + POST no endpoint anunciado (Fase 3).

Transporte bidirecional Server-Sent Events no padrão HTTP+SSE do MCP:

- a resposta chega por um stream GET de eventos (``text/event-stream``), lido
  continuamente por uma task em background;
- cada request JSON-RPC é enviado por um POST separado para a URL anunciada
  pelo próprio servidor no primeiro evento do stream (padrão ``endpoint`` do
  spec HTTP+SSE do MCP — ver :meth:`SseClient._capture_endpoint`). A URL
  anunciada é usada literalmente: referência relativa é resolvida com
  ``urljoin`` contra a URL da conexão do stream; URL absoluta é usada como
  veio. O cliente nunca recompõe a rota com template próprio (POSTs fora da
  URL anunciada viram 404 em servidores reais — ex.: mcp-proxy do VSCode);
- a correlação request→resposta é feita pelo id JSON-RPC presente no payload
  do evento — o Gateway mantém um contador único por client (o stdio também
  usa ids próprios, então ids do backend não servem de referência); o campo
  ``id:`` do protocolo SSE é ignorado de propósito (não é o id do JSON-RPC).

O evento lido no stream segue a forma ``data: {"jsonrpc":"2.0","id":N,...}``;
eventos sem ``id`` JSON-RPC (notificações do backend) são logados e ignorados.
O stream é considerado "pronto" quando o primeiro evento é processado — o
handshake ``initialize`` pode então ser enviado, já sabendo para onde POSTar.

Desconexões: se o stream cai no meio, todos os requests pendentes falham
imediatamente com ``BackendDisconnectedError`` (nunca ficam pendurados até o
timeout sem motivo) e o client se marca indisponível — o Health Monitor da
Fase 2 detecta e o auto-restart reabre a conexão.
"""

import asyncio
import json
from typing import Any, AsyncIterator
from urllib.parse import urljoin

import httpx
import structlog

from gateway.clients.base import (
    COMMENT_PREFIX,
    EVENT_DATA_PREFIX,
    JSON_CONTENT_TYPE,
    SSE_MEDIA_TYPE,
    BaseClient,
)
from gateway.config import BackendConfig
from gateway.errors import (
    BackendDisconnectedError,
    BackendError,
    BackendHttpStatusError,
    BackendTimeoutError,
)

logger = structlog.get_logger(__name__)

CONNECT_TIMEOUT_SECONDS = 5.0
READY_TIMEOUT_SECONDS = 10.0

JSON_HEADERS = {"Content-Type": JSON_CONTENT_TYPE, "Accept": JSON_CONTENT_TYPE}
SSE_STREAM_HEADERS = {"Accept": SSE_MEDIA_TYPE}

EVENT_ID_PREFIX = "id:"
EVENT_NAME_PREFIX = "event:"
STREAM_PATH = "/"

ENDPOINT_EVENT = "endpoint"
DEFAULT_POST_PATH = "/messages"
ENDPOINT_WAIT_SECONDS = 2.0


class SseClient(BaseClient):
    """Fala JSON-RPC com um backend MCP: POST para enviar, stream GET para receber.

    O GET do stream pede ``text/event-stream`` (``SSE_STREAM_HEADERS``), enquanto
    o POST fala JSON (``JSON_HEADERS``); headers custom do config valem para
    ambos. O destino dos POSTs (``_post_url``) é a URL anunciada pelo evento
    ``endpoint`` do stream — recapturada do zero a cada (re)conexão, pois a URL
    da conexão anterior pode ter expirado. ``READY_TIMEOUT_SECONDS`` limita
    quanto o ``start()`` espera pelo primeiro evento; ``ENDPOINT_WAIT_SECONDS`` é
    a janela para o servidor anunciar o ``endpoint`` antes do fallback legado.
    """

    def __init__(self, config: BackendConfig, request_timeout: float) -> None:
        super().__init__()
        self._config = config
        self._request_timeout = request_timeout
        self._http: httpx.AsyncClient | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._next_id = 0
        self._connected = False
        self._stopped = False
        self._ready = asyncio.Event()
        self._start_error: BackendError | None = None
        self._post_url: str = ""
        self._stream_url: str = ""

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Abre o stream SSE, captura o endpoint anunciado e faz o handshake.

        O stream é aberto antes do handshake (é por ele que a resposta do
        ``initialize`` chega), e o primeiro evento é lido antes de o client
        ficar "pronto": servidores MCP-SSE conformes anunciam nele a URL de POST
        da conexão (evento ``endpoint``). Se o backend não aceitar a conexão,
        não anunciar nada dentro do timeout ou o handshake estourar o timeout, o
        start falha — e o auto-restart da Fase 2 tenta de novo com backoff, como
        no stdio.

        O ``_post_url``/``_stream_url``/``_ready``/``_start_error`` são resetados
        a cada chamada: cada (re)conexão recaptura o endpoint do zero.
        """
        self._begin_start()
        self._stopped = False
        self._post_url = ""
        self._stream_url = ""
        self._ready = asyncio.Event()
        self._start_error = None
        self._http = httpx.AsyncClient(
            base_url=self._config.url or "",
            headers=self._config.headers,
            timeout=httpx.Timeout(self._request_timeout, connect=CONNECT_TIMEOUT_SECONDS),
        )
        self._reader_task = asyncio.create_task(
            self._read_stream(), name=f"sse-reader-{self._config.name}"
        )
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=READY_TIMEOUT_SECONDS)
        except asyncio.TimeoutError as exc:
            stream_opened = self._connected
            await self.stop()
            if stream_opened:
                raise BackendError(
                    f"backend '{self._config.name}': stream SSE aberto mas"
                    f" nenhum evento '{ENDPOINT_EVENT}' recebido em"
                    f" {READY_TIMEOUT_SECONDS}s (servidor não anunciou o endpoint)"
                ) from exc
            raise BackendError(
                f"backend '{self._config.name}': stream SSE não ficou pronto"
                f" em {READY_TIMEOUT_SECONDS}s"
            ) from exc
        if self._start_error is not None:
            await self.stop()
            raise self._start_error
        try:
            await self._initialize()
            self._mark_ready()
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        """Cancela a task de leitura e fecha o client HTTP (idempotente)."""
        if not self._begin_stop():
            return
        self._stopped = True
        try:
            self._fail_pending(
                BackendDisconnectedError(f"backend '{self._config.name}': cliente encerrado")
            )
            task = self._reader_task
            self._reader_task = None
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            http = self._http
            self._http = None
            self._connected = False
            if http is not None:
                await http.aclose()
        finally:
            self._mark_stopped()

    def is_alive(self) -> bool:
        """Vivo enquanto o stream SSE segue aberto (consulta sem I/O)."""
        return self._connected and self._http is not None

    # ------------------------------------------------------------------
    # Envio/recebimento JSON-RPC
    # ------------------------------------------------------------------

    async def send_request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """POST do request; a resposta chega pelo stream e é aguardada por id.

        Um POST respondido com 4xx/5xx falha imediatamente com
        ``BackendHttpStatusError`` (a resposta nunca viria pelo stream), sem
        esperar o timeout completo, e remove o pending — mas o stream em si
        segue aberto. Uma falha de rede no POST com o stream aberto indica
        backend morto: é tratada como desconexão (``_on_stream_closed``).
        """
        http = self._http
        if self._stopped or not self._connected or http is None:
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': stream SSE não está conectado"
            )
        self._next_id += 1
        request_id = self._next_id
        future = self._register_pending(request_id)
        try:
            response = await http.post(
                self._post_url,
                json={"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}},
                headers=JSON_HEADERS,
            )
        except httpx.TimeoutException as exc:
            self._pop_pending(request_id)
            raise BackendTimeoutError(
                f"backend '{self._config.name}': POST de '{method}' excedeu"
                f" {self._request_timeout}s"
            ) from exc
        except httpx.HTTPError as exc:
            self._pop_pending(request_id)
            await self._on_stream_closed(f"POST falhou ({exc.__class__.__name__})")
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': falha de rede no POST ({exc.__class__.__name__})"
            ) from exc
        if response.status_code >= 400:
            self._pop_pending(request_id)
            raise BackendHttpStatusError(response.status_code, method, backend=self._config.name)
        try:
            return await self._await_response(request_id, future, self._request_timeout, method)
        except BaseException:
            self._pop_pending(request_id)
            raise

    async def _send_notification(self, method: str, params: dict[str, Any] | None = None) -> None:
        """Notificação também vai por POST ao endpoint anunciado (sem resposta).

        Como ``httpx`` não levanta para 4xx/5xx e notificação não tem retorno
        esperado pelo protocolo, um erro HTTP do backend aqui é apenas logado
        (``sse_notification_falhou``), sem impactar o estado de saúde — o ping do
        monitor detecta uma queda real logo em seguida.
        """
        http = self._http
        if self._stopped or http is None:
            return
        try:
            response = await http.post(
                self._post_url,
                json={"jsonrpc": "2.0", "method": method, "params": params or {}},
                headers=JSON_HEADERS,
            )
        except httpx.HTTPError:
            logger.warning("sse_notification_falhou", backend=self._config.name, method=method)
            return
        if response.status_code >= 400:
            logger.warning(
                "sse_notification_falhou",
                backend=self._config.name,
                method=method,
                status_code=response.status_code,
            )

    # ------------------------------------------------------------------
    # Leitura do stream SSE (task em background)
    # ------------------------------------------------------------------

    async def _read_stream(self) -> None:
        """Abre o stream GET e consome eventos até a conexão cair.

        O "pronto" que o ``start()`` aguarda é o primeiro evento do stream
        processado (em ``_consume``): servidores conformes anunciam nesse evento
        a URL de POST da conexão; o handshake só sai depois.

        A URL do GET usa o path do stream literalmente: com ``base_url`` e
        ``GET /``, o httpx substituiria um path como ``/sse`` por ``/`` antes de
        o endpoint anunciado ser resolvido. A URL real da conexão
        (``response.url``) é guardada em ``_stream_url`` — é contra ela que o
        endpoint anunciado é resolvido.
        """
        http = self._http
        if http is None:
            self._set_start_error(BackendError(f"backend '{self._config.name}': sem client HTTP"))
            return
        reason: str
        try:
            stream_request_url = self._config.url or STREAM_PATH
            async with http.stream(
                "GET", stream_request_url, headers=SSE_STREAM_HEADERS
            ) as response:
                content_type = response.headers.get("content-type", "")
                if SSE_MEDIA_TYPE not in content_type:
                    self._set_start_error(
                        BackendError(
                            f"backend '{self._config.name}': resposta do stream não é SSE"
                            f" (content-type: {content_type or 'ausente'})"
                        )
                    )
                    return
                self._connected = True
                self._stream_url = str(response.url)
                await self._consume(response.aiter_lines())
                reason = "stream encerrado pelo backend"
        except asyncio.CancelledError:
            raise
        except httpx.HTTPError as exc:
            reason = f"erro de rede no stream ({exc.__class__.__name__})"
        except UnicodeDecodeError:
            reason = "stream com bytes UTF-8 inválidos"
        except Exception:
            logger.exception("erro inesperado lendo stream SSE", backend=self._config.name)
            reason = "erro inesperado lendo stream"
        await self._on_stream_closed(reason)

    def _capture_endpoint(self, data: str) -> None:
        """Guarda a URL anunciada pelo evento 'endpoint' como destino dos POSTs.

        O valor de ``data`` é usado literalmente — o servidor define a rota
        (geralmente com um ``session_id`` da conexão) e o cliente não reconstrói
        a URL com template próprio. A única transformação é a resolução da
        própria spec de URLs: referência relativa é resolvida com ``urljoin``
        contra a URL da conexão do stream (path absoluto, começando com ``/``,
        substitui o path inteiro — não herda o path do stream); URL absoluta
        (``http(s)://``) é usada exatamente como veio. POSTs fora da URL anunciada
        viram 404 em servidores reais — foi o sintoma do mcp-proxy do VSCode
        quando o cliente ainda recompunha ``/messages``.
        """
        raw = data.strip()
        if not raw:
            self._apply_fallback_endpoint("evento 'endpoint' sem data")
            return
        base = self._stream_url or self._config.url or ""
        self._post_url = urljoin(base, raw)
        logger.debug(
            "sse_endpoint_capturado",
            backend=self._config.name,
            endpoint_raw=raw,
            endpoint=self._post_url,
        )

    def _apply_fallback_endpoint(self, reason: str) -> None:
        """Fallback (modo legado): rota irmã do path da conexão do stream.

        Reproduz o comportamento anterior à captura do 'endpoint' — o fake legado
        da Fase 3 serve POST em ``/messages`` com o stream na raiz.
        """
        base = self._stream_url or self._config.url or ""
        self._post_url = urljoin(base, DEFAULT_POST_PATH.lstrip("/"))
        logger.debug(
            "sse_endpoint_fallback",
            backend=self._config.name,
            reason=reason,
            endpoint=self._post_url,
        )

    async def _consume(self, lines: AsyncIterator[str]) -> None:
        """Único consumidor das linhas do stream: resolve o endpoint e despacha.

        O ``aiter_lines()`` do httpx é um gerador único com buffer — iterá-lo
        duas vezes perderia linhas em buffer, então a resolução do evento
        ``endpoint`` acontece dentro deste mesmo loop (não em uma passada
        prévia).

        Padrão do transporte HTTP+SSE do MCP (spec legada): o primeiro evento do
        stream é nomeado ``endpoint`` e seu ``data`` é a URL (relativa ou
        absoluta, geralmente com um ``session_id`` próprio da conexão) que o
        cliente deve usar para todos os POSTs daquela conexão. A URL é usada
        literalmente (relativa resolvida com ``urljoin`` contra a URL da conexão
        do stream — nunca pelo merge do ``base_url`` do httpx, que prependa o
        path do stream e foi a causa do 404 no mcp-proxy do VSCode) e é
        recapturada do zero a cada (re)conexão.

        Compatibilidade (fallback documentado): servidores que não anunciam o
        ``endpoint`` — ex.: o fake legado da Fase 3 — caem de volta para a rota
        fixa ``/messages``. O fallback é decidido quando: (a) o primeiro evento
        do stream não é ``endpoint`` (o servidor fala, mas não anuncia — nesse
        caso o evento é despachado normalmente, pode ser uma mensagem real), ou
        (b) a janela ``ENDPOINT_WAIT_SECONDS`` expira tendo chegado linhas (ex.:
        keep-alives) mas nenhum evento. Se nenhuma linha chegar, o leitor fica
        parado e é o timeout do ``start()`` que falha com erro claro — stream
        mudo é servidor quebrado, não servidor antigo.
        """
        loop = asyncio.get_running_loop()
        endpoint_deadline = loop.time() + ENDPOINT_WAIT_SECONDS
        resolved = False
        data_lines: list[str] = []
        event_name: str | None = None

        def resolve_first_event() -> None:
            """Decide captura vs. fallback com o primeiro evento completo.

            Ao final marca ``_ready``: o primeiro evento foi processado
            (endpoint capturado ou fallback decidido), então o client está
            pronto para o handshake — é isso que o ``start()`` aguarda. Sem isso
            o start estouraria o timeout mesmo com o stream saudável.
            """
            nonlocal resolved
            resolved = True
            if event_name == ENDPOINT_EVENT and data_lines:
                self._capture_endpoint("\n".join(data_lines))
            else:
                self._apply_fallback_endpoint("nenhum evento 'endpoint' no início do stream")
                if data_lines:
                    self._dispatch_event("\n".join(data_lines), event_name)
            self._ready.set()

        async for line in lines:
            if not resolved and loop.time() >= endpoint_deadline:
                resolve_first_event()
            if line.strip() == "":
                if data_lines or event_name is not None:
                    if resolved:
                        self._dispatch_event("\n".join(data_lines), event_name)
                    else:
                        resolve_first_event()
                    data_lines = []
                    event_name = None
                continue
            if line.startswith(EVENT_ID_PREFIX) or line.startswith(COMMENT_PREFIX):
                continue
            if line.startswith(EVENT_NAME_PREFIX):
                event_name = line[len(EVENT_NAME_PREFIX) :].strip()
                continue
            if line.startswith(EVENT_DATA_PREFIX):
                data_lines.append(line[len(EVENT_DATA_PREFIX) :].strip())
        if not resolved:
            resolve_first_event()

    def _dispatch_event(self, data: str, event_name: str | None = None) -> None:
        """Parseia o JSON de um evento e resolve o pending correlacionado pelo id.

        Um reanúncio de ``endpoint`` no meio da conexão é ignorado (o spec define
        o endpoint como primeiro evento, já capturado). Eventos de controle
        não-JSON, não-dict, ou sem ``id`` JSON-RPC (notificações do backend) são
        logados em debug e descartados.
        """
        if event_name == ENDPOINT_EVENT:
            logger.debug("evento 'endpoint' ignorado no meio do stream", backend=self._config.name)
            return
        try:
            message: Any = json.loads(data)
        except json.JSONDecodeError:
            logger.debug("evento SSE não-JSON-RPC", backend=self._config.name)
            return
        if not isinstance(message, dict):
            logger.debug("evento SSE não-dict", backend=self._config.name)
            return
        request_id = message.get("id")
        if request_id is None or "method" in message:
            logger.debug(
                "notificação recebida via SSE",
                backend=self._config.name,
                method=message.get("method"),
            )
            return
        if not self._apply_response(message):
            logger.warning("resposta SSE inesperada", backend=self._config.name, id=request_id)

    async def _on_stream_closed(self, reason: str) -> None:
        """Stream caiu: marca indisponível e falha os pending imediatamente.

        O log ``sse_stream_perdido`` é emitido sempre que um stream já
        estabelecido termina por falha — inclusive quando a queda corre em
        paralelo a um ``stop()`` (ex.: o health monitor encerrando o client no
        shutdown enquanto o backend remoto morre): sem isso o evento de queda
        seria engolido e só ``backend_detected_offline`` apareceria, escondendo o
        motivo. A falha antes de o stream ficar pronto não loga aqui — o
        ``start()`` levanta o erro, que o manager loga como
        ``backend_start_failed``/``backend_restart_failed``.

        Se ``stop()`` já assumiu o estado (pending derrubados, conexão fechada),
        nada é limpo aqui. Se caiu antes de ficar pronto, registra o erro em
        ``_start_error`` e libera ``_ready`` para o ``start()`` desistir; caso
        contrário, derruba os pending com ``BackendDisconnectedError``.
        """
        if self._ready.is_set():
            logger.warning("sse_stream_perdido", backend=self._config.name, reason=reason)
        if self._stopped:
            return
        self._connected = False
        if not self._ready.is_set():
            self._set_start_error(
                BackendDisconnectedError(f"backend '{self._config.name}': {reason}")
            )
            self._ready.set()
            return
        self._fail_pending(
            BackendDisconnectedError(f"backend '{self._config.name}': stream SSE caiu ({reason})")
        )

    def _set_start_error(self, exc: BackendError) -> None:
        """Registra o primeiro erro de startup e libera ``_ready`` para o ``start()``."""
        if self._start_error is None:
            self._start_error = exc
        self._ready.set()
