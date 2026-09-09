"""SseClient: backend MCP remoto via SSE + POST no endpoint anunciado (Fase 3).

Transporte bidirecional "Server-Sent Events" no padrão do MCP:

- a resposta chega por um **stream GET** de eventos (``text/event-stream``),
  lido continuamente por uma task em background;
- cada request JSON-RPC é enviado por um **POST separado** para a URL
  anunciada pelo próprio servidor no PRIMEIRO evento do stream (padrão
  ``endpoint`` do spec HTTP+SSE do MCP — ver ``_capture_endpoint``). A URL
  anunciada é usada LITERALMENTE: relativa é resolvida com ``urljoin``
  contra a URL da conexão do stream; absoluta é usada como veio. O cliente
  nunca recomrota a rota com template próprio (POSTs fora da URL anunciada
  viram 404 em servidores reais — ex.: mcp-proxy do VSCode);
- a correlação request→resposta é feita pelo **id JSON-RPC** presente no
  payload da mensagem do evento — o Gateway mantém um contador único por
  client (o stdio também usa ids próprios, então ids do backend não servem
  de referência); o campo ``id:`` do protocolo SSE é ignorado de propósito
  (não é o id do JSON-RPC).

O evento lido no stream segue a forma ``data: {"jsonrpc":"2.0","id":N,...}``;
eventos sem ``id`` JSON-RPC (notificações do backend) são logados e ignorados.
O stream é considerado "pronto" quando o PRIMEIRO evento é processado — o
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
    backend_jsonrpc_error,
    set_exception_guarded,
)
from gateway.config import BackendConfig
from gateway.errors import (
    BackendDisconnectedError,
    BackendError,
    BackendTimeoutError,
)

logger = structlog.get_logger(__name__)

CONNECT_TIMEOUT_SECONDS = 5.0
READY_TIMEOUT_SECONDS = 10.0  # backend deve aceitar a conexão do stream neste prazo

JSON_HEADERS = {"Content-Type": JSON_CONTENT_TYPE, "Accept": JSON_CONTENT_TYPE}
# 4.1 — o GET do stream pede text/event-stream (não application/json, que era
# o Accept herdado do JSON_HEADERS em todo request deste client).
SSE_STREAM_HEADERS = {"Accept": SSE_MEDIA_TYPE}

EVENT_ID_PREFIX = "id:"
EVENT_NAME_PREFIX = "event:"
STREAM_PATH = "/"

ENDPOINT_EVENT = "endpoint"  # nome do primeiro evento do spec HTTP+SSE (MCP)
# Fallback para servidores que não anunciam 'endpoint' (modo legado): rota
# irmã do path da conexão do stream — igual ao comportamento pré-captura.
DEFAULT_POST_PATH = "/messages"
# Janela para o servidor anunciar o 'endpoint' antes do fallback (segundos).
# Servidores conformes anunciam imediatamente; a janela só custa tempo para
# os que não anunciam. Testes encurtam via monkeypatch.
ENDPOINT_WAIT_SECONDS = 2.0


class SseClient(BaseClient):
    """Fala JSON-RPC com um backend MCP: POST para enviar, stream GET para receber."""

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
        # Destino dos POSTs da conexão atual: a URL anunciada pelo evento
        # 'endpoint' do stream (resolvida contra a URL da conexão), ou o
        # fallback irmão do path do stream (servidor que não anuncia).
        # Resetado a cada start() — cada (re)conexão recaptura do zero.
        self._post_url: str = ""
        # URL real da conexão do stream (base do urljoin do endpoint).
        self._stream_url: str = ""

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Abre o stream SSE, captura o endpoint anunciado e faz o handshake.

        O stream é aberto ANTES do handshake (é por ele que a resposta do
        ``initialize`` vai chegar), e o PRIMEIRO evento é lido antes de o
        client ficar "pronto": servidores MCP-SSE conformes anunciam nele a
        URL de POST da conexão (evento ``endpoint``). Se o backend não
        aceitar a conexão, não anunciar nada dentro do timeout ou o
        handshake estourar o timeout, o start falha — e o auto-restart da
        Fase 2 tenta de novo com backoff, como no stdio.
        """
        if self._http is not None:
            return
        self._stopped = False
        self._post_url = ""  # recaptura do zero a cada conexão
        self._stream_url = ""
        self._ready = asyncio.Event()
        self._start_error = None
        # Sem headers default no AsyncClient: cada request declara os seus —
        # o POST fala JSON (JSON_HEADERS) e o GET do stream pede SSE
        # (SSE_STREAM_HEADERS, item 4.1). Headers custom do config valem para
        # ambos (mesclados por cima dos defaults em cada caso).
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
            stream_opened = self._connected  # stop() abaixo zera a flag
            await self.stop()
            if stream_opened:
                # Stream aberto mas o primeiro evento nunca chegou (servidor
                # conectou e ficou mudo): erro claro de conexão, sem travar.
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
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        """Cancela a task de leitura e fecha o client HTTP (idempotente)."""
        if self._stopped:
            return
        self._stopped = True
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

    def is_alive(self) -> bool:
        """Vivo enquanto o stream SSE segue aberto (consulta sem I/O)."""
        return self._connected and self._http is not None

    # ------------------------------------------------------------------
    # Envio/recebimento JSON-RPC
    # ------------------------------------------------------------------

    async def send_request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """POST do request; a resposta chega pelo stream e é aguardada por id."""
        http = self._http
        if self._stopped or not self._connected or http is None:
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': stream SSE não está conectado"
            )
        self._next_id += 1
        request_id = self._next_id
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            response = await http.post(
                self._post_url,
                json={"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}},
                headers=JSON_HEADERS,
            )
        except httpx.TimeoutException as exc:
            self._pending.pop(request_id, None)
            raise BackendTimeoutError(
                f"backend '{self._config.name}': POST de '{method}' excedeu"
                f" {self._request_timeout}s"
            ) from exc
        except httpx.HTTPError as exc:
            self._pending.pop(request_id, None)
            # POST falhar com o stream aberto indica backend morto: trata como
            # desconexão (os pending restantes também não terão resposta).
            await self._on_stream_closed(f"POST falhou ({exc.__class__.__name__})")
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': falha de rede no POST ({exc.__class__.__name__})"
            ) from exc
        # 4.2 — POST respondido com 4xx/5xx: a resposta NUNCA vai chegar pelo
        # stream; falha imediatamente (sem esperar o timeout completo) e remove
        # o pending. O stream em si segue aberto (o backend pode aceitar os
        # próximos POSTs).
        if response.status_code >= 400:
            self._pending.pop(request_id, None)
            raise BackendDisconnectedError(
                f"backend '{self._config.name}': HTTP {response.status_code}"
                f" no POST de '{method}'"
            )
        try:
            return await asyncio.wait_for(future, timeout=self._request_timeout)
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            raise BackendTimeoutError(
                f"backend '{self._config.name}': sem resposta a '{method}'"
                f" em {self._request_timeout}s (via stream SSE)"
            ) from None

    async def _send_notification(self, method: str, params: dict[str, Any] | None = None) -> None:
        """Notificação também vai por POST ao endpoint anunciado (sem resposta)."""
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
            logger.warning(
                "sse_notification_falhou", backend=self._config.name, method=method
            )
            return
        # 4.3 — httpx não levanta para 4xx/5xx: um erro HTTP do backend numa
        # notificação é silenciosamente ignorado sem esta checagem. Notificação
        # não tem retorno esperado pelo protocolo, então fica registrada em
        # log (não impacta o estado de saúde — o ping do monitor detecta uma
        # queda real logo em seguida).
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

        O "pronto" que o ``start()`` aguarda é o PRIMEIRO evento do stream
        processado (``_resolve_endpoint``): servidores conformes anunciam
        nesse evento a URL de POST da conexão; o handshake só sai depois.
        """
        http = self._http
        if http is None:
            self._set_start_error(BackendError(f"backend '{self._config.name}': sem client HTTP"))
            return
        reason: str
        try:
            # Use a URL configurada com o path do stream literalmente. Com
            # ``base_url`` e ``GET /``, httpx substitui um path como ``/sse``
            # por ``/`` antes de o endpoint anunciado ser resolvido.
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
                # URL real da conexão (o base_url do httpx pode acrescentar
                # path) — é contra ELA que o endpoint anunciado é resolvido.
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

        O valor de ``data`` é usado LITERALMENTE — o servidor é quem define a
        rota (geralmente com um ``session_id`` da conexão) e o cliente NÃO
        reconstrói a URL com template próprio. A única transformação é a
        resolução da própria spec de URLs: referência relativa é resolvida
        com ``urljoin`` contra a URL da CONEXÃO DO STREAM (path absoluto,
        começando com ``/``, substitui o path inteiro — não herda o path do
        stream); URL absoluta (``http(s)://``) é usada exatamente como veio.
        POSTs fora da URL anunciada viram 404 em servidores reais — foi
        exatamente o sintoma do mcp-proxy do VSCode quando o cliente ainda
        recom punha ``/messages``.
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

        Reproduz o comportamento anterior à captura do 'endpoint' — o fake
        legado da Fase 3 serve POST em ``/messages`` com o stream na raiz.
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

        O ``aiter_lines()`` do httpx é um gerador ÚNICO com buffer — iterá-lo
        duas vezes perderia linhas em buffer, então a resolução do evento
        ``endpoint`` acontece DENTRO deste mesmo loop (não em uma passada
        prévia).

        Padrão do transporte HTTP+SSE do MCP (spec legada): o PRIMEIRO evento
        do stream é nomeado ``endpoint`` e seu ``data`` é a URL (relativa ou
        absoluta, geralmente com um ``session_id`` próprio da conexão) que o
        cliente DEVE usar para TODOS os POSTs daquela conexão. A URL é usada
        literalmente (relativa resolvida com ``urljoin`` contra a URL da
        conexão do stream — NUNCA pelo merge do base_url do httpx, que
        prependa o path do stream e foi a causa do 404 no mcp-proxy do
        VSCode) e é recapturada do zero a cada (re)conexão — a da conexão
        anterior pode ter expirado.

        Compatibilidade (fallback documentado): servidores que NÃO anunciam
        o ``endpoint`` — ex.: o fake legado da Fase 3 — caem de volta para a
        rota fixa ``/messages``. O fallback é decidido quando: (a) o primeiro
        evento do stream não é ``endpoint`` (o servidor fala, mas não
        anuncia — nesse caso o evento é despachado normalmente, pode ser uma
        mensagem real), ou (b) a janela ``ENDPOINT_WAIT_SECONDS`` expira
        tendo chegado linhas (ex.: keep-alives) mas nenhum evento. Se
        NENHUMA linha chegar, o leitor fica parado e é o timeout do
        ``start()`` que falha com erro claro — stream mudo é servidor
        quebrado, não servidor antigo.
        """
        loop = asyncio.get_running_loop()
        endpoint_deadline = loop.time() + ENDPOINT_WAIT_SECONDS
        resolved = False
        data_lines: list[str] = []
        event_name: str | None = None

        def resolve_first_event() -> None:
            """Decide captura vs. fallback com o primeiro evento completo."""
            nonlocal resolved
            resolved = True
            if event_name == ENDPOINT_EVENT and data_lines:
                self._capture_endpoint("\n".join(data_lines))
            else:
                self._apply_fallback_endpoint(
                    "nenhum evento 'endpoint' no início do stream"
                )
                if data_lines:
                    # O primeiro evento pode ser uma mensagem real (ex.:
                    # notificação do servidor) — despacha normalmente.
                    self._dispatch_event("\n".join(data_lines), event_name)
            # O primeiro evento foi processado (endpoint capturado ou fallback
            # decidido): o client está pronto para o handshake — é isso que o
            # ``start()`` aguarda. Sem isso o start estouraria o timeout de
            # 10s mesmo com o stream saudável.
            self._ready.set()

        async for line in lines:
            if not resolved and loop.time() >= endpoint_deadline:
                # Janela expirou com linhas chegando (ex.: keep-alives) e
                # nenhum 'endpoint': fallback e segue consumindo normalmente.
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
                # 'id:' do protocolo SSE NÃO é o id do JSON-RPC — ignorado de
                # propósito; linhas ':' são comentários de keep-alive.
                continue
            if line.startswith(EVENT_NAME_PREFIX):
                event_name = line[len(EVENT_NAME_PREFIX) :].strip()
                continue
            if line.startswith(EVENT_DATA_PREFIX):
                data_lines.append(line[len(EVENT_DATA_PREFIX) :].strip())
        if not resolved:
            # Stream encerrou sem evento completo nenhum: fallback seguro.
            resolve_first_event()

    def _dispatch_event(self, data: str, event_name: str | None = None) -> None:
        """Parseia o JSON de um evento e resolve o pending correlacionado pelo id."""
        if event_name == ENDPOINT_EVENT:
            # Reanúncio no meio da conexão: o spec define o endpoint como
            # PRIMEIRO evento; aqui já capturamos (ou fizemos fallback).
            logger.debug("evento 'endpoint' ignorado no meio do stream", backend=self._config.name)
            return
        try:
            message: Any = json.loads(data)
        except json.JSONDecodeError:
            # Eventos de controle (ex.: anúncio do endpoint) não são JSON-RPC:
            # loga em debug e segue.
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
        future = self._pending.get(request_id)
        if future is None or future.done():
            logger.warning("resposta SSE inesperada", backend=self._config.name, id=request_id)
            return
        error = message.get("error")
        if error is not None:
            set_exception_guarded(
                future,
                backend_jsonrpc_error(error),
                backend=self._config.name,
            )
        else:
            future.set_result(message.get("result"))

    async def _on_stream_closed(self, reason: str) -> None:
        """Stream caiu: marca indisponível e falha os pending imediatamente.

        O log ``sse_stream_perdido`` é emitido SEMPRE que um stream já
        estabelecido termina por falha — inclusive quando a queda corre em
        paralelo a um ``stop()`` (ex.: o health monitor encerrando o client
        no shutdown enquanto o backend remoto morre): sem isso o evento de
        queda seria engolido e só ``backend_detected_offline`` apareceria,
        escondendo o motivo. A falha ANTES de o stream ficar pronto não loga
        aqui — o ``start()`` levanta o erro, que o manager loga como
        ``backend_start_failed``/``backend_restart_failed``.
        """
        if self._ready.is_set():
            logger.warning("sse_stream_perdido", backend=self._config.name, reason=reason)
        if self._stopped:
            # stop() já assumiu o estado (pending derrubados, conexão fechada):
            # nada a limpar, só não mexer em estado em desmonte.
            return
        self._connected = False
        if not self._ready.is_set():
            # Caiu antes de ficar pronto (ex.: conexão recusada no startup):
            # falha o start, que está aguardando _ready.
            self._set_start_error(
                BackendDisconnectedError(f"backend '{self._config.name}': {reason}")
            )
            self._ready.set()
            return
        self._fail_pending(
            BackendDisconnectedError(f"backend '{self._config.name}': stream SSE caiu ({reason})")
        )

    def _set_start_error(self, exc: BackendError) -> None:
        if self._start_error is None:
            self._start_error = exc
        self._ready.set()
