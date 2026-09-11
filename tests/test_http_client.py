"""Testes do HttpClient contra o fake_backend_http.py real (subprocesso)."""  
  
import json  
from typing import Any  
  
import httpx  
import pytest  
  
from conftest import capture_structlog_events, spawn_fake_server, stop_fake_server  
from gateway.clients.http_client import HttpClient  
from gateway.config import BackendConfig  
from gateway.errors import (  
    BackendDisconnectedError,  
    BackendError,  
    BackendHttpStatusError,  
    BackendJsonRpcError,  
    BackendTimeoutError,  
)  
  
  
@pytest.fixture  
def http_backend():  
    """Fake HTTP num subprocesso; derrubado no fim do teste."""  
    port, process = spawn_fake_server(__import__("conftest").FAKE_HTTP_BACKEND_PATH)  
    yield f"http://127.0.0.1:{port}"  
    stop_fake_server(process)  
  
  
def make_client(url: str, timeout: float = 5.0, headers: dict[str, str] | None = None) -> HttpClient:  
    config = BackendConfig(name="http-test", type="http", url=url, headers=headers or {})  
    return HttpClient(config, request_timeout=timeout)  
  
  
@pytest.mark.asyncio  
async def test_handshake_tools_list_e_call(http_backend: str) -> None:  
    """Handshake initialize contra o backend real, seguido de tools/list e call."""  
    client = make_client(http_backend)  
    await client.start()  
    try:  
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
        assert client.is_alive()  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_resources_e_prompts(http_backend: str) -> None:  
    client = make_client(http_backend)  
    await client.start()  
    try:  
        resources = await client.list_resources()  
        assert [r["uri"] for r in resources] == ["memory://greeting", "file:///tmp/fake-note.txt"]  
        read = await client.send_request("resources/read", {"uri": "memory://greeting"})  
        assert read["contents"][0]["text"] == "Ola! Bem-vindo ao fake backend."  
        prompts = await client.list_prompts()  
        assert [p["name"] for p in prompts] == ["greet"]  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_headers_customizados_do_config(http_backend: str) -> None:  
    """Headers declarados no config chegam ao backend em cada POST."""  
    client = make_client(http_backend, headers={"Authorization": "Bearer tok-teste"})  
    await client.start()  
    try:  
        await client.list_tools()  
        async with httpx.AsyncClient() as probe:  
            resp = await probe.get(f"{http_backend}/last-headers")  
        assert resp.json().get("authorization") == "Bearer tok-teste"  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_erro_quando_backend_nao_existe() -> None:  
    """Porta onde nada escuta vira BackendDisconnectedError (nunca exceção crua)."""  
    client = make_client("http://127.0.0.1:9")  
    with pytest.raises(BackendDisconnectedError):  
        await client.start()  
    await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_timeout_quando_backend_nao_responde() -> None:  
    """Fake com --delay maior que o timeout vira BackendTimeoutError no start."""  
    from conftest import FAKE_HTTP_BACKEND_PATH  
  
    port, process = spawn_fake_server(FAKE_HTTP_BACKEND_PATH, "--delay", "1.5")  
    try:  
        client = make_client(f"http://127.0.0.1:{port}", timeout=0.3)  
        with pytest.raises(BackendTimeoutError):  
            await client.start()  
        await client.stop()  
    finally:  
        stop_fake_server(process)  
  
  
@pytest.mark.asyncio  
async def test_erro_jsonrpc_do_backend(http_backend: str) -> None:  
    client = make_client(http_backend)  
    await client.start()  
    try:  
        with pytest.raises(BackendJsonRpcError) as exc_info:  
            await client.send_request("metodo/inexistente")  
        assert exc_info.value.code == -32603  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_stop_idempotente_e_is_alive_falso_depois(http_backend: str) -> None:  
    """stop() é idempotente e deixa o client inutilizável (is_alive falso)."""  
    client = make_client(http_backend)  
    await client.start()  
    await client.stop()  
    assert not client.is_alive()  
    await client.stop()  
    with pytest.raises(BackendDisconnectedError):  
        await client.send_request("ping", {})  
  
  
@pytest.mark.asyncio  
async def test_start_falho_nao_deixa_client_meio_pronto() -> None:  
    """Se o handshake falha, o stop é interno e o client fica inutilizável."""  
    client = make_client("http://127.0.0.1:9")  
    with pytest.raises(BackendError):  
        await client.start()  
    assert not client.is_alive()  
  
  
@pytest.mark.asyncio  
async def test_accept_streamable_http_em_toda_requisicao() -> None:  
    """Todo POST anuncia Accept de JSON e SSE (contrato do Streamable HTTP)."""  
    received: list[str] = []  
  
    def handler(request: httpx.Request) -> httpx.Response:  
        received.append(request.headers["accept"])  
        return httpx.Response(  
            200,  
            headers={"content-type": "application/json"},  
            json={"jsonrpc": "2.0", "id": 1, "result": {}},  
        )  
  
    client = make_client("http://test")  
    client._http = httpx.AsyncClient(  # noqa: SLF001  
        base_url="http://test", transport=httpx.MockTransport(handler)  
    )  
    try:  
        assert await client.send_request("ping") == {}  
        assert received == ["application/json, text/event-stream"]  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_resposta_streamable_http_em_sse() -> None:  
    """Resposta pode chegar como evento SSE (data:) e é parseada normalmente."""  
    def handler(request: httpx.Request) -> httpx.Response:  
        assert request.headers["accept"] == "application/json, text/event-stream"  
        payload = json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"ok": True}})  
        return httpx.Response(  
            200,  
            headers={"content-type": "text/event-stream"},  
            content=f": keep-alive\n\ndata: {payload}\n\n".encode(),  
        )  
  
    client = make_client("http://test")  
    client._http = httpx.AsyncClient(  # noqa: SLF001  
        base_url="http://test", transport=httpx.MockTransport(handler)  
    )  
    try:  
        assert await client.send_request("ping") == {"ok": True}  
    finally:  
        await client.stop()  
  
  
def _mock_client(handler: Any) -> HttpClient:  
    """HttpClient com transporte mockado (sem subprocesso)."""  
    client = make_client("http://test")  
    client._http = httpx.AsyncClient(  # noqa: SLF001  
        base_url="http://test", transport=httpx.MockTransport(handler)  
    )  
    return client  
  
  
@pytest.mark.asyncio  
async def test_ids_unicos_por_request() -> None:  
    """Cada request leva um id próprio (contador de instância)."""  
    ids: list[Any] = []  
  
    def handler(request: httpx.Request) -> httpx.Response:  
        payload = json.loads(request.content)  
        ids.append(payload["id"])  
        return httpx.Response(  
            200,  
            headers={"content-type": "application/json"},  
            json={"jsonrpc": "2.0", "id": payload["id"], "result": {}},  
        )  
  
    client = _mock_client(handler)  
    try:  
        await client.send_request("ping")  
        await client.send_request("ping")  
        assert ids == [1, 2]  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_resposta_sem_jsonrpc_é_rejeitada() -> None:  
    """Resposta sem o envelope jsonrpc 2.0 não é aceita como válida."""  
  
    def handler(request: httpx.Request) -> httpx.Response:  
        return httpx.Response(  
            200,  
            headers={"content-type": "application/json"},  
            json={"id": 1, "result": {}},  
        )  
  
    client = _mock_client(handler)  
    try:  
        with pytest.raises(BackendDisconnectedError, match="jsonrpc"):  
            await client.send_request("ping")  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_resposta_com_id_divergente_é_rejeitada_caminho_json() -> None:  
    """Caminho JSON direto: resposta com id diferente do enviado vira erro."""  
  
    def handler(request: httpx.Request) -> httpx.Response:  
        return httpx.Response(  
            200,  
            headers={"content-type": "application/json"},  
            json={"jsonrpc": "2.0", "id": 999, "result": {"ok": True}},  
        )  
  
    client = _mock_client(handler)  
    try:  
        with pytest.raises(BackendDisconnectedError, match="id inesperado"):  
            await client.send_request("ping")  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_resposta_com_id_divergente_nao_é_aceita_caminho_sse() -> None:  
    """Caminho SSE: evento com id de outra request não é usado como resposta."""  
  
    def handler(request: httpx.Request) -> httpx.Response:  
        payload = json.dumps({"jsonrpc": "2.0", "id": 999, "result": {"ok": True}})  
        return httpx.Response(  
            200,  
            headers={"content-type": "text/event-stream"},  
            content=f"data: {payload}\n\n".encode(),  
        )  
  
    client = _mock_client(handler)  
    try:  
        with pytest.raises(BackendDisconnectedError, match="sem resposta"):  
            await client.send_request("ping")  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_content_type_inesperado_é_erro_de_transporte() -> None:  
    """Content-Type fora de JSON/SSE falha na hora, com o tipo na mensagem."""  
  
    def handler(request: httpx.Request) -> httpx.Response:  
        return httpx.Response(  
            200, headers={"content-type": "text/plain"}, content=b"nao-sou-json"  
        )  
  
    client = _mock_client(handler)  
    try:  
        with pytest.raises(BackendDisconnectedError, match="Content-Type inesperado"):  
            await client.send_request("ping")  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_content_type_com_charset_é_aceito() -> None:  
    """Parâmetros como ; charset=utf-8 não quebram o match exato do Content-Type."""  
  
    def handler(request: httpx.Request) -> httpx.Response:  
        return httpx.Response(  
            200,  
            headers={"content-type": "application/json; charset=utf-8"},  
            json={"jsonrpc": "2.0", "id": 1, "result": {}},  
        )  
  
    client = _mock_client(handler)  
    try:  
        assert await client.send_request("ping") == {}  
    finally:  
        await client.stop()  
  
  
@pytest.mark.asyncio  
async def test_notificacao_com_erro_http_é_logada() -> None:  
    """POST de notificação respondido 4xx/5xx é registrado, não ignorado."""  
  
    def handler(request: httpx.Request) -> httpx.Response:  
        return httpx.Response(500)  
  
    client = _mock_client(handler)  
    try:  
        with capture_structlog_events() as events:  
            await client._send_notification("notifications/initialized")  # noqa: SLF001  
        warnings = [e for e in events if e["event"] == "http_notification_falhou"]  
        assert warnings, "notificação com erro HTTP deveria ser logada"  
        assert warnings[0].get("status_code") == 500  
    finally:  
        await client.stop()  
  
  
def test_headers_obrigatorios_vencem_config() -> None:  
    """Content-Type/Accept do transporte vencem os do config (deliberado)."""  
    client = make_client(  
        "http://test",  
        headers={"Content-Type": "text/plain", "Accept": "text/plain", "X-Custom": "v"},  
    )  
    headers = client._post_headers()  # noqa: SLF001  
    assert headers["Content-Type"] == "application/json"  
    assert headers["Accept"] == "application/json, text/event-stream"  
    assert headers["X-Custom"] == "v"  
  
  
@pytest.mark.asyncio  
async def test_erro_http_status_levanta_backend_http_status_error() -> None:  
    """HTTP >= 400 levanta BackendHttpStatusError com status_code programático.  
  
    Antes da correção, todo erro HTTP >= 400 virava BackendDisconnectedError  
    genérico — a informação do código (401, 404, 500 ...) só estava na string.  
    Agora é uma subclasse com o código acessível programaticamente.  
    """  
    def handler(request: httpx.Request) -> httpx.Response:  
        return httpx.Response(401, headers={"content-type": "application/json"})  
  
    client = _mock_client(handler)  
    try:  
        with pytest.raises(BackendHttpStatusError) as exc_info:  
            await client.send_request("ping")  
        assert exc_info.value.status_code == 401  
        assert exc_info.value.method == "ping"  
        assert client._config.name in str(exc_info.value)  
    finally:  
        await client.stop()  
  
  
def test_backend_http_status_error_eh_subclasse_de_backend_disconnected() -> None:  
    """BackendHttpStatusError é subclasse de BackendDisconnectedError.  
  
    Garante compatibilidade: código que trata BackendDisconnectedError continua  
    funcionando (catch no server.py / BackendManager), enquanto código novo  
    pode fazer isinstance(exc, BackendHttpStatusError) para diferenciar  
    401/403 (config) de falha de rede.  
    """  
    exc = BackendHttpStatusError(500, "tools/list", backend="test-backend")  
    assert isinstance(exc, BackendDisconnectedError)  
    assert isinstance(exc, BackendError)  
    assert exc.status_code == 500  
    assert exc.method == "tools/list"  
    assert exc.backend == "test-backend"  
    assert "500" in str(exc)  
    assert "tools/list" in str(exc)