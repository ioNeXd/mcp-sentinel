"""Testes das rotas HTTP: POST /mcp, GET /health e GET /api/servers."""

import json
import sys
from typing import Any, Iterator

import httpx
import pytest
import structlog

from conftest import (
    FAKE_BACKEND_PATH,
    FakeClient,
    capture_structlog_events,
    configure_quiet_structlog,
    make_manager_for_clients,
)
from gateway.clients.stdio_client import StdioClient
from gateway.config import BackendConfig, GatewayConfig
from gateway.http_server import _render_dashboard, _validate_new_backend_payload, create_app
from gateway.registries import PromptRegistry, ResourceRegistry, ToolRegistry
from gateway.server import DIAGNOSTIC_TOOL_NAME, McpServer

ECHO_TOOL = {
    "name": "echo",
    "description": "Repete texto.",
    "inputSchema": {"type": "object", "properties": {}},
}
ADD_TOOL = {
    "name": "add",
    "description": "Soma.",
    "inputSchema": {"type": "object", "properties": {}},
}

BASE_URL = "http://test"


def test_render_dashboard_importado_com_backend() -> None:
    html = _render_dashboard(
        {
            "status": "ok",
            "backends": {"backend-a": "running"},
            "tools_count": 1,
            "resources_count": 0,
            "prompts_count": 0,
        },
        [
            {
                "name": "backend-a",
                "type": "stdio",
                "status": "RUNNING",
                "tools_count": 1,
                "resources_count": 0,
                "prompts_count": 0,
                "consecutive_failures": 0,
            }
        ],
        token=None,
        gw_endpoint="http://127.0.0.1:8080/mcp",
    )
    assert 'class="card"' in html
    assert 'data-name="backend-a"' in html


def test_primeiro_paint_e_versao_reduzida_dos_cards() -> None:
    """O primeiro paint (server-rendered) NÃO é o shape completo do renderCard.

    Regressão da docstring (item 31): ela afirmava "mesmo shape que renderCard
    gera no JS", mas o card server-rendered nunca tem ``card-remove`` nem
    ``pin-btn`` — o de fixar é impossível no servidor (o estado dos fixados
    vive no localStorage do navegador), e ambos chegam no primeiro refresh.
    O contrato documentado agora: mesma ESTRUTURA visual, versão REDUZIDA.
    """
    html_out = _render_dashboard(
        {
            "status": "ok",
            "backends": {},
            "tools_count": 0,
            "resources_count": 0,
            "prompts_count": 0,
        },
        [
            {
                "name": "backend-a",
                "type": "stdio",
                "status": "running",
                "tools_count": 1,
                "resources_count": 0,
                "prompts_count": 0,
                "consecutive_failures": 0,
            }
        ],
        token=None,
        gw_endpoint="http://127.0.0.1:8080/mcp",
    )
    card_region = html_out.split('data-name="backend-a"', 1)[1][:1200]

    # Estrutura presente no primeiro paint:
    assert "badge-running" in card_region
    assert 'data-action="restart"' in card_region
    assert 'data-action="disable"' in card_region
    assert 'data-action="enable"' in card_region

    # Ações ausentes por design (chegam no primeiro refresh):
    assert "card-remove" not in card_region
    assert "pin-btn" not in card_region


async def make_app_with_fakes() -> McpServer:
    """McpServer com dois clients fake, já iniciado."""
    client_a = FakeClient(tools=[ECHO_TOOL])
    client_b = FakeClient(tools=[ADD_TOOL])
    manager, registries = make_manager_for_clients({"backend-a": client_a, "backend-b": client_b})
    server = McpServer(manager, registries)
    await server.start()
    return server


@pytest.mark.asyncio
async def test_tools_list_via_http() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
            )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["id"] == 1
        assert payload.get("error") is None
        names = {tool["name"] for tool in payload["result"]["tools"]}
        assert names == {"backend-a.echo", "backend-b.add", DIAGNOSTIC_TOOL_NAME}
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tools_call_via_http() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "backend-b.add", "arguments": {"a": 2, "b": 3}},
                },
            )
        assert resp.status_code == 200
        assert resp.json()["result"]["content"][0]["text"] == "resultado de add"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_json_malformado_retorna_parse_error() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp",
                content="{isto nao e json",
                headers={"content-type": "application/json"},
            )
        assert resp.status_code == 400
        payload = resp.json()
        assert payload["error"]["code"] == -32700
        assert payload["id"] is None
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_content_type_invalido() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post("/mcp", content="qualquer coisa")
        assert resp.status_code == 415
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_content_type_exige_match_exato_e_aceita_charset() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            invalid = await client.post(
                "/mcp",
                content="{}",
                headers={"content-type": "not-application/json-anything"},
            )
            valid = await client.post(
                "/mcp",
                content='{"jsonrpc":"2.0","id":1,"method":"ping"}',
                headers={"content-type": "application/json; charset=utf-8"},
            )
        assert invalid.status_code == 415
        assert valid.status_code == 200
        assert valid.json()["id"] == 1
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_body_nao_dict_retorna_invalid_request() -> None:
    """Body que não é objeto JSON (ex.: batch/lista) vira InvalidRequest."""
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post("/mcp", json=[1, 2, 3])
        assert resp.status_code == 200
        assert resp.json()["error"]["code"] == -32600
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_notificacao_retorna_202_sem_corpo() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"}
            )
        assert resp.status_code == 202
        assert resp.content == b""
    finally:
        await server.stop()


@pytest.fixture
def captured_logs() -> Iterator[list[dict[str, Any]]]:
    """Captura os eventos do structlog durante um request HTTP.

    Reconfigura o structlog para anexar os eventos numa lista (mantendo o
    merge_contextvars, para o request_id aparecer em cada evento) e restaura o
    baseline silencioso do conftest ao final.
    """
    events: list[dict[str, Any]] = []

    def capture(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        events.append(event_dict)
        return event_dict

    structlog.configure(
        processors=[structlog.contextvars.merge_contextvars, capture],
        wrapper_class=structlog.BoundLogger,
        logger_factory=structlog.ReturnLoggerFactory(),
    )
    try:
        yield events
    finally:
        configure_quiet_structlog()


@pytest.mark.asyncio
async def test_campo_obrigatorio_ausente_via_http() -> None:
    """Request sem 'method' vira InvalidRequest (-32600), preservando o id."""
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 7})
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["error"]["code"] == -32600
        assert payload["id"] == 7
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_payload_grande_demais_retorna_413() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server, max_payload_bytes=100)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp",
                content="x" * 500,
                headers={"content-type": "application/json"},
            )
        assert resp.status_code == 413
    finally:
        await server.stop()


class _ChunkedBody(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b'{"jsonrpc":"2.0","id":1,"method":"'
        yield b'ping"}'


@pytest.mark.asyncio
async def test_payload_chunked_excede_limite_durante_stream() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server, max_payload_bytes=10)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp",
                content=_ChunkedBody(),
                headers={"content-type": "application/json"},
            )
        assert resp.status_code == 413
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_erro_interno_nao_vaza_detalhes_para_cliente() -> None:
    server = await make_app_with_fakes()
    original = server.process_message

    async def fail_process_message(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("segredo interno")

    server.process_message = fail_process_message  # type: ignore[method-assign]
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
            )
        assert resp.status_code == 200
        assert resp.json()["error"]["message"] == "Internal error"
        assert "segredo interno" not in resp.text
    finally:
        server.process_message = original  # type: ignore[method-assign]
        await server.stop()


@pytest.mark.asyncio
async def test_auth_401_quando_token_esperado_e_header_ausente() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
            )
        assert resp.status_code == 401
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_auth_401_com_token_errado() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                headers={"Authorization": "Bearer errado"},
            )
        assert resp.status_code == 401
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_auth_ok_com_token_correto() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                headers={"Authorization": "Bearer segredo"},
            )
        assert resp.status_code == 200
        assert resp.json().get("error") is None
    finally:
        await server.stop()


async def _post_tools_list(client: httpx.AsyncClient, **kwargs: Any) -> httpx.Response:
    return await client.post(
        "/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, **kwargs
    )


@pytest.mark.asyncio
async def test_auth_ok_com_token_via_query_string() -> None:
    """?token= correto, sem header nenhum -> sucesso.

    Autenticação via query string (``?token=``) foi adicionada para contornar
    o escaping quebrado no Windows (cmd.exe corrompe headers com espaço em
    argumentos de npx/mcp-remote).
    """
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await _post_tools_list(client, params={"token": "segredo"})
        assert resp.status_code == 200
        payload = resp.json()
        assert payload.get("error") is None
        assert {t["name"] for t in payload["result"]["tools"]} == {
            "backend-a.echo",
            "backend-b.add",
            DIAGNOSTIC_TOOL_NAME,
        }
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_auth_401_com_token_errado_via_query_string() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await _post_tools_list(client, params={"token": "errado"})
        assert resp.status_code == 401
        assert resp.headers["www-authenticate"] == "Bearer"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_auth_ok_com_header_errado_e_query_correta() -> None:
    """Ambas as formas enviadas, só a query correta -> aceito.

    Basta UMA forma bater (documentado no README como decisão de precedência).
    """
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await _post_tools_list(
                client,
                params={"token": "segredo"},
                headers={"Authorization": "Bearer errado"},
            )
        assert resp.status_code == 200
        assert resp.json().get("error") is None
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_auth_ok_com_header_correto_e_query_errada() -> None:
    """Recíproco: header correto + query errada -> aceito.

    O header correto já autentica; a query errada não derruba o acesso.
    """
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await _post_tools_list(
                client,
                params={"token": "errado"},
                headers={"Authorization": "Bearer segredo"},
            )
        assert resp.status_code == 200
        assert resp.json().get("error") is None
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_token_via_query_string_nao_vaza_nos_logs() -> None:
    """O valor do ?token= não pode aparecer em texto puro em NENHUM evento
    do structlog — nem em request aceita, nem em 401 (regressão de vazamento).
    """
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo-secreto")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            with capture_structlog_events() as events:
                await _post_tools_list(client, params={"token": "segredo-secreto"})
                await _post_tools_list(client, params={"token": "segredo-errado"})
        assert events  # a request gerou logs — a asserção não é vácuo
        for event in events:
            serialized = json.dumps(event, default=str)
            assert "segredo-secreto" not in serialized
            assert "segredo-errado" not in serialized
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_request_id_consistente_nos_logs_da_mesma_request(
    captured_logs: list[dict[str, Any]],
) -> None:
    """Todo log de uma request HTTP carrega o mesmo request_id (contextvars).

    Duas requisições distintas geram dois request_ids distintos e consistentes;
    a tools/call também produz um log de roteamento com o backend escolhido.
    """
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            await client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
            await client.post(
                "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "backend-a.echo", "arguments": {}},
                },
            )
        events = [e for e in captured_logs if e.get("request_id") is not None]
        assert events  # os requests produziram logs com request_id
        groups: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            groups.setdefault(str(event["request_id"]), []).append(event)
        assert len(groups) == 2
        for request_events in groups.values():
            names = {e["event"] for e in request_events}
            assert "jsonrpc_request_received" in names
            assert "http_request_completed" in names
        assert any(
            e.get("event") == "request_dispatched" and e.get("backend") == "backend-a"
            for events_for_request in groups.values()
            for e in events_for_request
        )
        assert any(
            e.get("event") == "http_request_completed" and e.get("duration_ms") is not None
            for events_for_request in groups.values()
            for e in events_for_request
        )
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_e2e_dois_backends_reais_via_gateway() -> None:
    """Critério de aceite da Fase 0+1: tools, resources e prompts de 2 backends.

    Cobre o round-trip completo por HTTP: tools/list+call, resources/list+read
    (URI namespaced) e prompts/list+get, roteando para os backends reais.
    """
    clients = {
        name: StdioClient(
            BackendConfig(name=name, command=sys.executable, args=[str(FAKE_BACKEND_PATH)])
        )
        for name in ("backend-a", "backend-b")
    }
    config = GatewayConfig(
        backends=[
            BackendConfig(name=name, command=sys.executable, args=[str(FAKE_BACKEND_PATH)])
            for name in ("backend-a", "backend-b")
        ],
        health_check_interval_seconds=3600.0,
    )
    registries = (ToolRegistry(), ResourceRegistry(), PromptRegistry())
    from gateway.backend_manager import BackendManager

    manager = BackendManager(config, registries)
    manager._create_client = lambda backend_config: clients[backend_config.name]  # type: ignore[method-assign,return-value,union-attr]
    server = McpServer(manager, registries)
    await server.start()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
            )
            assert resp.status_code == 200
            names = {tool["name"] for tool in resp.json()["result"]["tools"]}
            assert names == {
                "backend-a.echo",
                "backend-a.add",
                "backend-b.echo",
                "backend-b.add",
                DIAGNOSTIC_TOOL_NAME,  # tool nativa do Gateway, sempre injetada
            }

            resp = await client.post(
                "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "backend-b.echo", "arguments": {"text": "ola"}},
                },
            )
            assert resp.status_code == 200
            payload = resp.json()
            assert payload.get("error") is None
            assert payload["result"]["content"][0]["text"] == "ola"

            resp = await client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 3, "method": "resources/list"}
            )
            assert resp.status_code == 200
            uris = {r["uri"] for r in resp.json()["result"]["resources"] if isinstance(r, dict)}
            assert "backend-a.memory://greeting" in uris
            assert "backend-b.file:///tmp/fake-note.txt" in uris

            resp = await client.post(
                "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "resources/read",
                    "params": {"uri": "backend-a.memory://greeting"},
                },
            )
            assert resp.status_code == 200
            read_payload = resp.json()
            assert read_payload.get("error") is None
            assert read_payload["result"]["contents"][0]["uri"] == "backend-a.memory://greeting"

            resp = await client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 5, "method": "prompts/list"}
            )
            assert resp.status_code == 200
            prompt_names = {
                p["name"] for p in resp.json()["result"]["prompts"] if isinstance(p, dict)
            }
            assert "backend-a.greet" in prompt_names

            resp = await client.post(
                "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": 6,
                    "method": "prompts/get",
                    "params": {"name": "backend-a.greet", "arguments": {"person": "Ana"}},
                },
            )
            assert resp.status_code == 200
            get_payload = resp.json()
            assert get_payload.get("error") is None
            assert get_payload["result"]["messages"][0]["content"]["text"] == "Ola, Ana!"
    finally:
        await server.stop()


async def make_client(url: str, app: Any) -> httpx.AsyncClient:
    """Client httpx já configurado com o transporte ASGI do app."""
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=url)


@pytest.mark.asyncio
async def test_health_sem_auth_mesmo_com_token_configurado() -> None:
    """GET /health NUNCA exige auth (checagens de infraestrutura) — decisão da Fase 2."""
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with await make_client(BASE_URL, app) as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["status"] == "ok"
        assert payload["backends"] == {
            "total": 2,
            "running": 2,
            "offline": 0,
            "restarting": 0,
            "failed": 0,
            "disabled": 0,
        }
        assert payload["tools_count"] == 2
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_health_degraded_quando_backend_offline() -> None:
    """Backend offline (sem restart possível) reflete 'degraded' no /health."""
    client_a = FakeClient(tools=[ECHO_TOOL])
    client_b = FakeClient(tools=[ADD_TOOL])
    manager, registries = make_manager_for_clients({"backend-a": client_a, "backend-b": client_b})
    manager.config.auto_restart = False
    server = McpServer(manager, registries)
    await server.start()
    try:
        # Derruba o backend-a: next check marca offline (sem auto-restart).
        await client_a.stop()
        await manager.check_and_recover("backend-a")

        app = create_app(server)
        async with await make_client(BASE_URL, app) as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["status"] == "degraded"
        assert payload["backends"]["running"] == 1
        assert payload["backends"]["offline"] == 1
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_api_servers_sem_auth_retorna_401_com_token_configurado() -> None:
    """GET /api/servers respeita a mesma auth do POST /mcp (decisão da Fase 2)."""
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with await make_client(BASE_URL, app) as client:
            resp = await client.get("/api/servers")
            assert resp.status_code == 401
            assert resp.headers["www-authenticate"] == "Bearer"

            resp = await client.get("/api/servers", headers={"Authorization": "Bearer segredo"})
        assert resp.status_code == 200
        servers = resp.json()["servers"]
        assert {s["name"] for s in servers} == {"backend-a", "backend-b"}
        for entry in servers:
            assert entry["status"] == "running"
            assert entry["consecutive_failures"] == 0
            assert entry["command"] == "fake"
            assert entry["tools_count"] == 1
    finally:
        await server.stop()


def test_build_backend_entry_padroniza_type_stdio_explicito() -> None:
    """O painel "Adicionar MCP" e o importador emitem o MESMO shape para stdio.

    Regressão: ``_build_backend_entry`` omitia ``type`` para stdio (espelhando
    o ``backend-a`` do config original) enquanto o ``convert_entry`` do
    importador sempre emite ``type: "stdio"`` — o mesmo config.json ficava
    heterogêneo conforme o caminho que criou cada entrada. Unificado COM type
    explícito (a forma documentada do importador no README); o schema aceita
    as duas formas (default ``stdio``), então a mudança é cosmética.
    """
    import importlib.util
    from pathlib import Path

    from gateway.http_server import _build_backend_entry

    # Shape do painel: type explícito; args só quando há argumentos.
    assert _build_backend_entry({"name": "novo", "command": "python", "args": []}) == {
        "name": "novo",
        "type": "stdio",
        "command": "python",
    }
    assert _build_backend_entry({"name": "r", "type": "http", "url": "http://x:1/"}) == {
        "name": "r",
        "type": "http",
        "url": "http://x:1/",
    }

    # Paridade com o importador — os dois escritores do MESMO arquivo:
    spec = importlib.util.spec_from_file_location(
        "import_claude_desktop_config",
        Path(__file__).resolve().parents[1] / "scripts" / "import_claude_desktop_config.py",
    )
    assert spec is not None and spec.loader is not None
    importer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(importer)
    dashboard = _build_backend_entry({"name": "fs", "command": "python", "args": ["-u"]})
    importado, _ = importer.convert_entry("fs", {"command": "python", "args": ["-u"]})
    assert dashboard == importado


def test_validacao_de_nome_rejeita_nao_ascii_igual_ao_schema() -> None:
    """A rota e o schema Pydantic validam 'name' pelo MESMO padrão ASCII.

    Regressão: a rota usava ``c.isalnum()`` (Unicode-aware), que aceitava
    acentos ('café') que o ``BACKEND_NAME_PATTERN`` do schema rejeita — o
    backend era GRAVADO no config.json e o Gateway não subia no boot
    seguinte. O accept da rota nunca pode divergir do schema.
    """
    from gateway.config import BackendConfig

    for name in ("café", "münchen", "backend.a", "backend a", ""):
        payload = {"name": name, "command": "python", "args": []}
        error = _validate_new_backend_payload(payload)
        assert error is not None, f"nome inválido aceito pela rota: {name!r}"
        # E o mesmo payload nunca validaria no schema (a rota não é mais
        # permissiva que ele, em nenhuma direção):
        entry = {"name": name, "command": "python", "args": []}
        with pytest.raises(Exception):
            BackendConfig(**entry)

    # Nome válido continua passando na rota E no schema:
    valid = {"name": "backend-ok_1", "command": "python", "args": []}
    assert _validate_new_backend_payload(valid) is None
    BackendConfig(**valid)  # não levanta


@pytest.mark.asyncio
async def test_api_servers_sem_token_configurado_aberto() -> None:
    """Sem auth_token no config, /api/servers fica aberto (comportamento local)."""
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with await make_client(BASE_URL, app) as client:
            resp = await client.get("/api/servers")
        assert resp.status_code == 200
        assert len(resp.json()["servers"]) == 2
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_add_backend_valida_contra_o_schema_antes_de_gravar() -> None:
    """POST /api/config/backends valida a entrada contra o BackendConfig ANTES
    de gravar no config.json (fonte única de verdade).

    Regressão: a checagem leve da rota (name por regex, url por startswith)
    era mais permissiva que o schema do boot — ex.: ``url='http://'`` (sem
    host) passava na rota, era GRAVADO no config.json e o Gateway não subia
    no boot seguinte. O accept da rota nunca pode ser mais permissivo que o
    schema, em NENHUM campo (o caso do 'name' já tinha sido fechado antes).
    """
    import os
    import tempfile

    from gateway.config import GatewayConfig

    server = await make_app_with_fakes()
    tmpdir = tempfile.TemporaryDirectory()
    config_path = os.path.join(tmpdir.name, "config.json")
    with open(config_path, "w", encoding="utf-8") as f:
        # uma entrada pré-existente, para provar que ela sobrevive intacta
        json.dump({"backends": [{"name": "backend-a", "command": str(FAKE_BACKEND_PATH)}]}, f)
    previous = os.environ.get("MCP_GATEWAY_CONFIG")
    os.environ["MCP_GATEWAY_CONFIG"] = config_path
    try:
        app = create_app(server)
        async with await make_client(BASE_URL, app) as client:
            # 1) url sem host: passava na checagem leve (startswith), era
            # gravado e quebrava o boot. Agora: 422 ANTES de tocar o disco.
            resp = await client.post(
                "/api/config/backends",
                json={"name": "backend-x", "type": "http", "url": "http://"},
            )
            assert resp.status_code == 422
            assert "schema" in resp.json()["detail"]

            # 2) o disco não foi tocado pela entrada rejeitada
            with open(config_path, encoding="utf-8") as f:
                on_disk = json.load(f)
            assert [b["name"] for b in on_disk["backends"]] == ["backend-a"]

            # 3) payload válido: grava, e o arquivo inteiro valida contra o
            # MESMO schema que o load_config usa no boot.
            resp = await client.post(
                "/api/config/backends",
                json={
                    "name": "backend-ok",
                    "type": "http",
                    "url": "http://127.0.0.1:9000/mcp",
                },
            )
            assert resp.status_code == 200
            with open(config_path, encoding="utf-8") as f:
                on_disk = json.load(f)
            assert [b["name"] for b in on_disk["backends"]] == [
                "backend-a",
                "backend-ok",
            ]
            GatewayConfig.model_validate(on_disk)  # o boot não rejeitaria
    finally:
        if previous is None:
            os.environ.pop("MCP_GATEWAY_CONFIG", None)
        else:
            os.environ["MCP_GATEWAY_CONFIG"] = previous
        await server.stop()


@pytest.mark.asyncio
async def test_delete_ultimo_backend_e_recusado_com_409() -> None:
    """DELETE /api/servers/{name} recusa remover o ÚLTIMO backend (409).

    Regressão: a GatewayConfig exige ao menos um backend no boot (verificação
    do item 24) — sem a guarda, remover o último pelo painel gravava
    ``"backends": []`` no disco e o Gateway não subia de novo. A guarda roda
    ANTES de qualquer efeito: memória e disco ficam intactos.
    """
    import os
    import tempfile

    server = await make_app_with_fakes()
    tmpdir = tempfile.TemporaryDirectory()
    config_path = os.path.join(tmpdir.name, "config.json")
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump({"backends": [{"name": "backend-a", "command": "python"}]}, f)
    previous = os.environ.get("MCP_GATEWAY_CONFIG")
    os.environ["MCP_GATEWAY_CONFIG"] = config_path
    try:
        app = create_app(server)
        async with await make_client(BASE_URL, app) as client:
            resp = await client.delete("/api/servers/backend-a")
            assert resp.status_code == 409
            assert "último backend" in resp.json()["detail"]

            # Memória e disco intactos (a guarda roda antes de qualquer efeito):
            resp = await client.get("/api/servers")
            assert {s["name"] for s in resp.json()["servers"]} == {
                "backend-a",
                "backend-b",
            }
            with open(config_path, encoding="utf-8") as f:
                assert [b["name"] for b in json.load(f)["backends"]] == ["backend-a"]

            # Backend que não está no disco: a guarda (baseada no disco) não
            # bloqueia — fluxo antigo de memória segue.
            resp = await client.delete("/api/servers/backend-b")
            assert resp.status_code == 200
    finally:
        if previous is None:
            os.environ.pop("MCP_GATEWAY_CONFIG", None)
        else:
            os.environ["MCP_GATEWAY_CONFIG"] = previous
        await server.stop()


@pytest.mark.asyncio
async def test_import_claude_desktop_valida_o_config_mesclado_antes_de_gravar() -> None:
    """POST /api/import/claude-desktop valida o resultado mesclado (regressão).

    Mesmo portão do restore/settings: o ``import_config``/``convert_entry``
    NÃO valida contra o schema — ex.: ``url`` sem host passa verbatim — então
    sem o ``GatewayConfig.model_validate`` antes da gravação uma entrada
    importada envenenava o config.json e quebrava o boot seguinte (o restore
    sempre teve esse portão; o import, não).
    """
    import os
    import tempfile

    from gateway.config import GatewayConfig

    server = await make_app_with_fakes()
    tmpdir = tempfile.TemporaryDirectory()
    config_path = os.path.join(tmpdir.name, "config.json")
    source_path = os.path.join(tmpdir.name, "claude_desktop_config.json")
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump({"backends": [{"name": "backend-a", "command": "python"}]}, f)
    previous = os.environ.get("MCP_GATEWAY_CONFIG")
    os.environ["MCP_GATEWAY_CONFIG"] = config_path
    try:
        app = create_app(server)
        async with await make_client(BASE_URL, app) as client:
            # 1) importação válida: grava, e o arquivo inteiro valida contra
            # o MESMO schema que o load_config usa no boot.
            with open(source_path, "w", encoding="utf-8") as f:
                json.dump({"mcpServers": {"fs": {"command": "python", "args": []}}}, f)
            resp = await client.post("/api/import/claude-desktop", json={"path": source_path})
            assert resp.status_code == 200, resp.text
            with open(config_path, encoding="utf-8") as f:
                on_disk = json.load(f)
            assert [b["name"] for b in on_disk["backends"]] == ["backend-a", "fs"]
            GatewayConfig.model_validate(on_disk)

            # 2) importação com entrada que o convert_entry não consegue
            # checar (url sem host): 422 e o disco NÃO é tocado.
            with open(source_path, "w", encoding="utf-8") as f:
                json.dump({"mcpServers": {"ruim": {"type": "http", "url": "http://"}}}, f)
            resp = await client.post("/api/import/claude-desktop", json={"path": source_path})
            assert resp.status_code == 422
            assert "nada foi gravado" in resp.json()["detail"]
            with open(config_path, encoding="utf-8") as f:
                on_disk = json.load(f)
            assert [b["name"] for b in on_disk["backends"]] == ["backend-a", "fs"]
    finally:
        if previous is None:
            os.environ.pop("MCP_GATEWAY_CONFIG", None)
        else:
            os.environ["MCP_GATEWAY_CONFIG"] = previous
        await server.stop()


def test_dashboard_js_escapa_campos_livres_de_config() -> None:
    """Regressão de DOM XSS no refresh dos cards (achado 29).

    O ``renderCard`` do dashboard despeja ``fmtMeta(s)`` — command/url/args,
    campos SEM restrição de schema (só o name é ASCII) — direto no
    ``grid.innerHTML``. O JS embutido precisa escapar todo campo antes de
    interpolar; o caminho server-rendered da página de detalhe já escapava
    (``html.escape``), este teste trava o client-side.

    Cobertura por ponto único: os cards do grupo "Fixados" e dos grupos por
    status passam pelo MESMO ``renderCard`` (``pinned.map(renderCard)``),
    então o escape na função + no ``fmtMeta`` cobre todos os caminhos de
    card de uma vez. Os demais interpolados no ``renderGrid`` (``group.label``,
    "📌 Fixados", ``.length``) são estáticos/numéricos — não são vetor.
    """
    import os
    import re
    import shutil
    import subprocess
    import tempfile

    page = _render_dashboard(
        {
            "status": "ok",
            "backends": {},
            "tools_count": 0,
            "resources_count": 0,
            "prompts_count": 0,
        },
        [],
        token=None,
        gw_endpoint="http://127.0.0.1:8080/mcp",
    )

    # O helper existe e é aplicado nos pontos que interpolam dados da API:
    assert "function escapeHtml" in page
    assert "escapeHtml(s.name)" in page  # renderCard: name em texto e atributos
    assert "escapeHtml(bits.join" in page  # fmtMeta: command/url/args crus
    assert "document.createTextNode(h.status)" in page  # pill sem interpolação

    # Prova funcional: extrai o escapeHtml do JS RENDERIZADO e verifica o
    # mapa completo (& < > " ') — neutraliza payload de XSS clássico.
    node = shutil.which("node")
    if node is None:
        pytest.skip("node não disponível para executar o JS embutido")
    js = re.search(r"<script>(.*)</script>", page, re.S).group(1)
    js = js.replace("{{", "{").replace("}}", "}")
    start = js.index("function escapeHtml")
    depth, i = 0, js.index("{", start)
    for i in range(i, len(js)):
        if js[i] == "{":
            depth += 1
        elif js[i] == "}":
            depth -= 1
            if depth == 0:
                escape_fn = js[start : i + 1]
                break
    harness = f"{escape_fn}\nconsole.log(escapeHtml(process.argv[2]));"
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(harness)
        harness_path = f.name
    try:
        payload = "<img src=x onerror=alert(1)> \"'&"
        result = subprocess.run(
            [node, harness_path, payload], capture_output=True, text=True, timeout=30
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == ("&lt;img src=x onerror=alert(1)&gt; &quot;&#39;&amp;")
        assert "<" not in result.stdout and '"' not in result.stdout
    finally:
        os.unlink(harness_path)


@pytest.mark.parametrize(
    ("servers", "confirm_script", "fetch_rejects", "expect"),
    [
        pytest.param(
            [{"name": "a", "status": "running"}, {"name": "b", "status": "offline"}],
            [True],
            False,
            {"confirms": 1, "fetch": True, "warn_name": None, "body": True},
            id="fluxo-feliz-dispara-o-post",
        ),
        pytest.param(
            [{"name": "b", "status": "restarting"}],
            [True, True],
            False,
            {"confirms": 2, "fetch": True, "warn_name": "b", "body": True},
            id="backend-em-restart-pede-confirmacao-extra",
        ),
        pytest.param(
            [{"name": "a", "status": "running"}],
            [False],
            False,
            {"confirms": 1, "fetch": False, "warn_name": None, "body": False},
            id="confirm-recusado-nao-dispara-nada",
        ),
        pytest.param(
            [{"name": "b", "status": "restarting"}],
            [True, False],
            False,
            {"confirms": 2, "fetch": False, "warn_name": "b", "body": False},
            id="confirm-extra-recusado-nao-dispara",
        ),
        pytest.param(
            [{"name": "a", "status": "running"}],
            [True],
            True,
            {"confirms": 1, "fetch": True, "warn_name": None, "body": True},
            id="conexao-cai-antes-da-resposta-e-esperado",
        ),
    ],
)
def test_handler_de_shutdown_executa_e_dispara_o_post(
    servers: list[dict[str, str]],
    confirm_script: list[bool],
    fetch_rejects: bool,
    expect: dict[str, object],
) -> None:
    """O handler do botão "Sair do MCP" roda sem erro e dispara o POST.

    Regressão do achado 30 (que era FALSO contra a árvore, mas o handler é o
    único trecho de JS do dashboard sem guarda automatizada de execução —
    ``node --check`` não pega ReferenceError, que é erro de runtime).

    Extrai o handler REAL da página renderizada (``_render_dashboard``) e o
    executa em node com stubs de ``confirm``/``fetch``/``document``.
    Contratos travados:

    - a variável ``restarting`` existe no escopo e deriva de ``lastServers``;
    - backend em restart pede confirmação EXTRA citando o nome dele;
    - recusa (em qualquer confirmação) não dispara o POST;
    - o POST vai para ``/api/shutdown`` com método POST e headers de auth;
    - conexão cair antes da resposta NÃO quebra o handler (try/catch é
      deliberado) — título e corpo ainda refletem o encerramento;
    - após o POST, título e ``body.innerHTML`` mostram o estado encerrado.
    """
    import os
    import re
    import shutil
    import subprocess
    import tempfile

    page = _render_dashboard(
        {
            "status": "ok",
            "backends": {},
            "tools_count": 0,
            "resources_count": 0,
            "prompts_count": 0,
        },
        [],
        token=None,
        gw_endpoint="http://127.0.0.1:8080/mcp",
    )
    node = shutil.which("node")
    if node is None:
        pytest.skip("node não disponível para executar o JS embutido")

    js = re.search(r"<script>(.*)</script>", page, re.S).group(1)
    js = js.replace("{{", "{").replace("}}", "}")

    # Extrai a instrução addEventListener do handler de shutdown inteira
    # (do marker até o ``});`` que fecha a chamada, via contagem de chaves).
    marker = 'document.getElementById("shutdown-gateway").addEventListener'
    i = js.index(marker)
    depth, j = 0, js.index("{", i)
    for j in range(j, len(js)):
        if js[j] == "{":
            depth += 1
        elif js[j] == "}":
            depth -= 1
            if depth == 0:
                break
    k = js.index(");", j)
    snippet = js[i : k + 2]
    assert snippet.endswith("});")

    harness = (
        """
const calls = { confirm: [], fetch: [] };
const confirmScript = JSON.parse(process.argv[3]);
const FETCH_REJECTS = process.argv[4] === "1";
globalThis.confirm = (msg) => {
  calls.confirm.push(String(msg));
  return confirmScript.length > 1 ? confirmScript.shift() : confirmScript[0];
};
globalThis.fetch = async (url, opts) => {
  calls.fetch.push({ url: String(url), method: (opts && opts.method) || "GET", hasAuth: !!(opts && opts.headers) });
  if (FETCH_REJECTS) throw new Error("connection dropped");
  return { ok: true };
};
const listeners = {};
globalThis.document = {
  title: "",
  body: {},
  getElementById: (id) => ({ addEventListener: (ev, cb) => { listeners[id + ":" + ev] = cb; } }),
};
const lastServers = JSON.parse(process.argv[2]);
const AUTH_HEADERS = { Authorization: "Bearer teste" };
"""
        + snippet
        + """
(async () => {
  const handler = listeners["shutdown-gateway:click"];
  if (!handler) { console.error("LISTENER_NAO_REGISTRADO"); process.exit(1); }
  await handler();
  console.log("RESULT_JSON=" + JSON.stringify(calls));
  console.log("TITLE_JSON=" + JSON.stringify(document.title));
  console.log("BODY_JSON=" + JSON.stringify(String(document.body.innerHTML || "")));
})().catch((e) => { console.error("HARNESS_ERROR=" + (e && e.message)); process.exit(1); });
"""
    )
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(harness)
        harness_path = f.name
    try:
        result = subprocess.run(
            [
                node,
                harness_path,
                json.dumps(servers),
                json.dumps(confirm_script),
                "1" if fetch_rejects else "0",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        lines = dict(
            line.split("=", 1) for line in result.stdout.strip().splitlines() if "=" in line
        )
        calls = json.loads(lines["RESULT_JSON"])

        assert len(calls["confirm"]) == expect["confirms"]
        if expect["warn_name"] is not None and expect["confirms"] > 1:
            # A confirmação extra cita o backend em restart pelo nome.
            assert expect["warn_name"] in calls["confirm"][1]

        assert len(calls["fetch"]) == (1 if expect["fetch"] else 0)
        if expect["fetch"]:
            post = calls["fetch"][0]
            assert post["url"] == "/api/shutdown"
            assert post["method"] == "POST"
            assert post["hasAuth"] is True

        assert json.loads(lines["TITLE_JSON"]) == (
            "\u23fb Gateway encerrado" if expect["fetch"] else ""
        )
        body = json.loads(lines["BODY_JSON"])
        assert ("Gateway encerrado" in body) is bool(expect["body"])
    finally:
        os.unlink(harness_path)
