"""Testes do SseClient: fake subprocesso (spec HTTP+SSE) + fakes in-process.

Os fakes in-process (``_echo_sse_app``) cobrem combinações de paths que não
valem flags no fake subprocesso: endpoint em path diferente do stream, URL
relativa vs. absoluta — os casos de resolução de URL da correção pós-Fase 5.
"""

import asyncio
import json
import threading
import time
from typing import Any, AsyncIterator

import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

import fake_logic
from conftest import (
    FAKE_SSE_BACKEND_PATH,
    capture_structlog_events,
    spawn_fake_server,
    stop_fake_server,
)
from gateway.clients import sse_client as sse_client_module
from gateway.clients.sse_client import SseClient
from gateway.config import BackendConfig
from gateway.errors import (
    BackendDisconnectedError,
    BackendError,
    BackendHttpStatusError,
    BackendJsonRpcError,
    BackendTimeoutError,
)


@pytest.fixture
def sse_backend():
    """Fake SSE num subprocesso; derrubado no fim do teste."""
    port, process = spawn_fake_server(FAKE_SSE_BACKEND_PATH)
    yield f"http://127.0.0.1:{port}"
    stop_fake_server(process)


def make_client(url: str, timeout: float = 5.0) -> SseClient:
    config = BackendConfig(name="sse-test", type="sse", url=url)
    return SseClient(config, request_timeout=timeout)


@pytest.mark.asyncio
async def test_handshake_tools_list_e_call(sse_backend: str) -> None:
    """POST /messages envia; a resposta chega pelo stream, correlacionada por id."""
    client = make_client(sse_backend)
    await client.start()
    try:
        assert client.is_alive()  # stream aberto

        tools = await client.list_tools()
        assert {tool["name"] for tool in tools} == {"echo", "add"}

        result = await client.send_request(
            "tools/call", {"name": "echo", "arguments": {"text": "ola"}}
        )
        assert result["content"][0]["text"] == "ola"

        result = await client.send_request(
            "tools/call", {"name": "add", "arguments": {"a": 2, "b": 3}}
        )
        assert result["content"][0]["text"] == "5"
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_requests_concorrentes_sem_trocar_respostas(sse_backend: str) -> None:
    """Vários requests no ar: cada future recebe a resposta do SEU id."""
    client = make_client(sse_backend)
    await client.start()
    try:
        results = await asyncio.gather(
            client.send_request("tools/call", {"name": "echo", "arguments": {"text": "um"}}),
            client.send_request("tools/call", {"name": "add", "arguments": {"a": 1, "b": 2}}),
            client.send_request("tools/call", {"name": "echo", "arguments": {"text": "três"}}),
        )
        texts = [r["content"][0]["text"] for r in results]
        assert texts == ["um", "3", "três"]
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_erro_jsonrpc_do_backend_via_stream(sse_backend: str) -> None:
    """Erro JSON-RPC chega pelo stream e é levantado no send_request correto."""
    client = make_client(sse_backend)
    await client.start()
    try:
        with pytest.raises(BackendJsonRpcError) as exc_info:
            await client.send_request("metodo/inexistente")
        assert exc_info.value.code == -32603
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_backend_inacessivel_falha_o_start() -> None:
    """Porta onde nada escuta: stream não abre → start falha com erro claro."""
    client = make_client("http://127.0.0.1:9")
    with pytest.raises((BackendError, BackendDisconnectedError)):
        await client.start()
    await client.stop()


@pytest.mark.asyncio
async def test_stream_que_cai_falha_requests_pendentes() -> None:
    """Stream morre no meio: pending falha IMEDIATAMENTE, não fica pendurado."""
    # --delay 2: o request fica no ar o suficiente para o stream cair primeiro.
    port, process = spawn_fake_server(FAKE_SSE_BACKEND_PATH, "--delay", "2")
    client = make_client(f"http://127.0.0.1:{port}", timeout=10.0)
    await client.start()
    try:
        task = asyncio.create_task(
            client.send_request("tools/call", {"name": "echo", "arguments": {"text": "x"}})
        )
        await asyncio.sleep(0.3)  # POST já saiu; resposta só chegaria em ~2s
        assert not task.done()
        stop_fake_server(process)

        started = asyncio.get_running_loop().time()
        with pytest.raises(BackendDisconnectedError):
            await task
        elapsed = asyncio.get_running_loop().time() - started
        # Falhou na hora (stream fechado detectado), não no timeout de 10s.
        assert elapsed < 5.0
        assert not client.is_alive()
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_timeout_quando_backend_nao_responde() -> None:
    """Backend vivo mas lento (--delay > timeout) → BackendTimeoutError."""
    port, process = spawn_fake_server(FAKE_SSE_BACKEND_PATH, "--delay", "1.5")
    try:
        client = make_client(f"http://127.0.0.1:{port}", timeout=0.3)
        with pytest.raises(BackendTimeoutError):
            await client.start()  # handshake initialize estoura o timeout
        await client.stop()
    finally:
        stop_fake_server(process)


@pytest.mark.asyncio
async def test_stop_cancela_stream_e_idempotente(sse_backend: str) -> None:
    client = make_client(sse_backend)
    await client.start()
    task = client._reader_task
    assert task is not None
    await client.stop()
    assert task.done()
    assert not client.is_alive()
    await client.stop()  # idempotente


@pytest.mark.asyncio
async def test_notificacoes_do_handshake_nao_quebram(sse_backend: str) -> None:
    """notifications/initialized vai por POST sem esperar resposta no stream."""
    client = make_client(sse_backend)
    await client.start()  # inclui o POST da notificação; não pode travar
    try:
        result = await client.send_request("ping", {})
        assert result == {}
    finally:
        await client.stop()


# ----------------------------------------------------------------------
# Evento 'endpoint' do spec HTTP+SSE (correção pós-Fase 5)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_endpoint_event_capturado_e_usado_no_post(sse_backend: str) -> None:
    """O primeiro evento 'endpoint' define o destino de TODOS os POSTs.

    O fake valida o session_id anunciado e responde 404 para POSTs fora da
    URL anunciada — se o cliente não seguisse o anúncio (bug antigo: POST
    fixo em /messages), o tools/call abaixo falharia.
    """
    client = make_client(sse_backend)
    await client.start()
    try:
        assert client._post_url.startswith(sse_backend + "/messages?session_id=")  # noqa: SLF001
        tools = await client.list_tools()
        assert {tool["name"] for tool in tools} == {"echo", "add"}
        result = await client.send_request(
            "tools/call", {"name": "echo", "arguments": {"text": "ola"}}
        )
        assert result["content"][0]["text"] == "ola"
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_reconexao_recaptura_novo_endpoint(sse_backend: str) -> None:
    """stop()+start() recaptura o endpoint do zero — o antigo pode expirar."""
    client = make_client(sse_backend)
    await client.start()
    primeiro = client._post_url  # noqa: SLF001
    await client.stop()
    await client.start()
    try:
        # Nova conexão → novo session_id do fake → novo endpoint anunciado.
        assert client._post_url != primeiro  # noqa: SLF001
        assert await client.send_request("ping", {}) == {}
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_fallback_sem_evento_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Servidor que não anuncia 'endpoint' (modo legado): cai para /messages.

    Keep-alives chegam dentro da janela (prova que o servidor está falando),
    mas nenhum evento 'endpoint' → fallback para a rota fixa, logado em
    debug (``sse_endpoint_fallback``), e tudo continua funcionando.
    """
    monkeypatch.setattr(sse_client_module, "ENDPOINT_WAIT_SECONDS", 0.3)
    port, process = spawn_fake_server(FAKE_SSE_BACKEND_PATH, "--no-endpoint", "--keepalive", "0.1")
    try:
        client = make_client(f"http://127.0.0.1:{port}")
        with capture_structlog_events() as events:
            await client.start()
        try:
            assert client._post_url.endswith("/messages")  # noqa: SLF001
            fallbacks = [e for e in events if e["event"] == "sse_endpoint_fallback"]
            assert fallbacks, "fallback deveria ser logado"
            tools = await client.list_tools()
            assert {tool["name"] for tool in tools} == {"echo", "add"}
        finally:
            await client.stop()
    finally:
        stop_fake_server(process)


@pytest.mark.asyncio
async def test_timeout_servidor_silencioso(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stream abre mas nada é enviado: erro claro no start, sem travar."""
    monkeypatch.setattr(sse_client_module, "READY_TIMEOUT_SECONDS", 1.0)
    port, process = spawn_fake_server(FAKE_SSE_BACKEND_PATH, "--silent")
    try:
        client = make_client(f"http://127.0.0.1:{port}")
        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(BackendError) as exc_info:
            await client.start()
        assert loop.time() - started < 5.0  # falhou no timeout curto, não pendurado
        assert "endpoint" in str(exc_info.value)
        assert not client.is_alive()
    finally:
        stop_fake_server(process)


# ----------------------------------------------------------------------
# URL literal do endpoint (correção: não recompor com template próprio)
# ----------------------------------------------------------------------


def _start_inprocess_sse_server(
    app: "FastAPI",
) -> tuple[str, "uvicorn.Server", threading.Thread]:
    """Sobe um app FastAPI in-process (porta efêmera, thread própria).

    Usado pelos fakes mínimos dos testes de resolução de URL — cada caso
    precisa de rotas diferentes (stream em path próprio, endpoint em outro
    path), que não valem flags no fake subprocesso.
    """
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    return f"http://127.0.0.1:{port}", server, thread


def _stop_inprocess_sse_server(server: "uvicorn.Server", thread: "threading.Thread") -> None:
    server.should_exit = True
    thread.join(timeout=5)


def _echo_sse_app(
    *, stream_path: str, endpoint_event_data: str, messages_path: str
) -> FastAPI:
    """App SSE mínimo: anuncia ``endpoint_event_data`` e responde via stream.

    O POST é aceito em ``messages_path`` e a resposta JSON-RPC volta pelo
    stream (o cliente não lê o corpo do POST) — mesmo contrato do fake
    subprocesso, em qualquer combinação de paths.
    """
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    queues: list[asyncio.Queue[str]] = []

    @app.get(stream_path)
    async def sse(request: Request) -> StreamingResponse:
        queue: asyncio.Queue[str] = asyncio.Queue()
        queues.append(queue)

        async def gen() -> AsyncIterator[str]:
            try:
                endpoint_data = endpoint_event_data.replace(
                    "{origin}", str(request.base_url).rstrip("/")
                )
                yield f"event: endpoint\ndata: {endpoint_data}\n\n"
                while True:
                    try:
                        data = await asyncio.wait_for(queue.get(), timeout=0.2)
                        yield f"data: {data}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keep-alive\n\n"
            finally:
                queues.remove(queue)

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.post(messages_path)
    async def messages(request: Request) -> JSONResponse:
        body: Any = await request.json()
        if fake_logic.is_notification(body):
            return JSONResponse(content={"received": True})
        for queue in queues:
            queue.put_nowait(json.dumps(fake_logic.build_response(body)))
        return JSONResponse(content={"received": True})

    return app


@pytest.mark.asyncio
async def test_endpoint_absoluto_substitui_path_do_stream() -> None:
    """Endpoint '/message?sessionId=x' com stream em '/sse': origem + path novo.

    O caso real do mcp-proxy do VSCode: o evento anunciava uma rota própria,
    mas o merge do base_url do httpx prependava o path do stream e o POST
    caía em ``/sse/messages`` (404). O endpoint com path absoluto (começando
    com ``/``) substitui o path inteiro — e a resposta chega pelo stream.
    """
    url, server, thread = _start_inprocess_sse_server(
        _echo_sse_app(
            stream_path="/sse",
            endpoint_event_data="/message?sessionId=abc",
            messages_path="/message",
        )
    )
    try:
        client = make_client(url + "/sse")
        await client.start()
        try:
            assert client._post_url == url + "/message?sessionId=abc"  # noqa: SLF001
            result = await client.send_request("ping", {})
            assert result == {}
        finally:
            await client.stop()
    finally:
        _stop_inprocess_sse_server(server, thread)


@pytest.mark.asyncio
async def test_endpoint_relativo_simples_resolve_contra_path_do_stream() -> None:
    """Endpoint sem '/' inicial: resolvido contra o path da conexão (urljoin)."""
    url, server, thread = _start_inprocess_sse_server(
        _echo_sse_app(
            stream_path="/api/sse",
            endpoint_event_data="messages?session_id=abc",
            messages_path="/api/messages",
        )
    )
    try:
        client = make_client(url + "/api/sse")
        await client.start()
        try:
            # urljoin('http://h/api/sse', 'messages?...') → 'http://h/api/messages?...'
            assert client._post_url == url + "/api/messages?session_id=abc"  # noqa: SLF001
            result = await client.send_request("ping", {})
            assert result == {}
        finally:
            await client.stop()
    finally:
        _stop_inprocess_sse_server(server, thread)


@pytest.mark.asyncio
async def test_endpoint_url_absoluta_usada_literalmente() -> None:
    """URL absoluta (http://) no evento: usada exatamente como veio."""
    url, server, thread = _start_inprocess_sse_server(
        _echo_sse_app(
            stream_path="/",
            endpoint_event_data="{origin}/absolute-message?sid=1",
            messages_path="/absolute-message",
        )
    )
    try:
        client = make_client(url)
        await client.start()
        try:
            # Absoluta: nem merge do base_url nem urljoin alteram o valor.
            assert client._post_url == url + "/absolute-message?sid=1"  # noqa: SLF001
            assert await client.send_request("ping", {}) == {}
        finally:
            await client.stop()
    finally:
        _stop_inprocess_sse_server(server, thread)


# ----------------------------------------------------------------------
# Robustez de transporte (headers por request, status do POST, notificação)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_do_stream_pede_sse_e_post_pede_json(sse_backend: str) -> None:
    """4.1 — GET do stream pede text/event-stream; POST fala application/json.

    O fake devolve os headers do último POST em /last-headers (mesma rota do
    fake HTTP); o Accept do GET do stream é verificado via app in-process.
    """
    url, server, thread = _start_inprocess_sse_server(
        _accept_probe_app(stream_path="/sse", messages_path="/message")
    )
    try:
        client = make_client(url + "/sse")
        await client.start()
        try:
            assert await client.send_request("ping", {}) == {}
        finally:
            await client.stop()
        assert _ACCEPTS["stream"] == "text/event-stream"
        assert _ACCEPTS["post"] == "application/json"
    finally:
        _stop_inprocess_sse_server(server, thread)


_ACCEPTS: dict[str, str] = {}


def _accept_probe_app(*, stream_path: str, messages_path: str) -> "FastAPI":
    """App SSE que registra o Accept do GET do stream e do POST."""
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    queues: list[asyncio.Queue[str]] = []

    @app.get(stream_path)
    async def sse(request: Request) -> StreamingResponse:
        queue: asyncio.Queue[str] = asyncio.Queue()
        queues.append(queue)
        _ACCEPTS["stream"] = request.headers.get("accept", "")

        async def gen() -> AsyncIterator[str]:
            try:
                yield f"event: endpoint\ndata: {messages_path}?sid=1\n\n"
                while True:
                    try:
                        data = await asyncio.wait_for(queue.get(), timeout=0.2)
                        yield f"data: {data}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keep-alive\n\n"
            finally:
                queues.remove(queue)

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.post(messages_path)
    async def messages(request: Request) -> JSONResponse:
        _ACCEPTS["post"] = request.headers.get("accept", "")
        body: Any = await request.json()
        if fake_logic.is_notification(body):
            return JSONResponse(content={"received": True})
        for queue in queues:
            queue.put_nowait(json.dumps(fake_logic.build_response(body)))
        return JSONResponse(content={"received": True})

    return app


@pytest.mark.asyncio
async def test_post_com_erro_http_falha_imediatamente(sse_backend: str) -> None:
    """4.2 — POST respondido 4xx/5xx: erro na hora, sem esperar o timeout.

    Item 3 (91-120): o SSE client usa BackendHttpStatusError (subclasse de
    BackendDisconnectedError) para diferenciar programaçaticamente 401/404/500
    de falha de rede. O pytest.raises continua pegando BackendDisconnectedError
    (compatibilidade), mas verificamos também o tipo específico e o status_code.
    """
    url, server, thread = _start_inprocess_sse_server(
        _post_error_app(status_code=404, stream_path="/sse", messages_path="/message")
    )
    try:
        client = make_client(url + "/sse", timeout=5.0)
        await client.start()
        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(BackendHttpStatusError, match="HTTP 404") as exc_info:
            await client.send_request("ping", {})
        elapsed = loop.time() - started
        assert elapsed < 2.0  # falhou imediatamente, não no timeout de 5s
        # BackendHttpStatusError carrega o código HTTP acessível programaticamente.
        assert exc_info.value.status_code == 404
        # E continua sendo BackendDisconnectedError para quem trata pelo tipo genérico.
        assert isinstance(exc_info.value, BackendDisconnectedError)
    finally:
        await client.stop()
        _stop_inprocess_sse_server(server, thread)


def _post_error_app(
    *, status_code: int, stream_path: str, messages_path: str
) -> "FastAPI":
    """App SSE que responde erro HTTP a todo POST exceto o handshake initialize.

    O initialize precisa ser respondido (pelo stream) para o start() do client
    completar; os DEMAIS métodos recebem o status de erro — é o cenário de
    backend que aceita a conexão mas rejeita requests/notificações com 4xx/5xx.
    """
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    queues: list[asyncio.Queue[str]] = []

    @app.get(stream_path)
    async def sse() -> StreamingResponse:
        queue: asyncio.Queue[str] = asyncio.Queue()
        queues.append(queue)

        async def gen() -> AsyncIterator[str]:
            try:
                yield f"event: endpoint\ndata: {messages_path}?sid=1\n\n"
                while True:
                    try:
                        data = await asyncio.wait_for(queue.get(), timeout=0.2)
                        yield f"data: {data}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keep-alive\n\n"
            finally:
                queues.remove(queue)

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.post(messages_path)
    async def messages(request: Request) -> JSONResponse:
        body: Any = await request.json()
        if body.get("method") == "initialize":
            for queue in queues:
                queue.put_nowait(json.dumps(fake_logic.build_response(body)))
            return JSONResponse(content={"received": True})
        return JSONResponse(status_code=status_code, content={"detail": "erro"})

    return app


@pytest.mark.asyncio
async def test_notificacao_com_erro_http_é_logada(sse_backend: str) -> None:
    """4.3 — POST de notificação respondido 4xx/5xx é registrado, não ignorado."""
    url, server, thread = _start_inprocess_sse_server(
        _post_error_app(status_code=500, stream_path="/sse", messages_path="/message")
    )
    try:
        client = make_client(url + "/sse")
        await client.start()
        with capture_structlog_events() as events:
            await client._send_notification("notifications/initialized")  # noqa: SLF001
        warnings = [e for e in events if e["event"] == "sse_notification_falhou"]
        assert warnings, "notificação com erro HTTP deveria ser logada"
        assert warnings[0].get("status_code") == 500
    finally:
        await client.stop()
        _stop_inprocess_sse_server(server, thread)
