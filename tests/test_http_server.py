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
from gateway.http_server import create_app
from gateway.registries import PromptRegistry, ResourceRegistry, ToolRegistry
from gateway.server import McpServer

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


async def make_app_with_fakes() -> McpServer:
    """McpServer com dois clients fake, já iniciado."""
    client_a = FakeClient(tools=[ECHO_TOOL])
    client_b = FakeClient(tools=[ADD_TOOL])
    manager, registries = make_manager_for_clients(
        {"backend-a": client_a, "backend-b": client_b}
    )
    server = McpServer(manager, registries)
    await server.start()
    return server


@pytest.mark.asyncio
async def test_tools_list_via_http() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
            resp = await client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
            )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["id"] == 1
        assert payload.get("error") is None
        names = {tool["name"] for tool in payload["result"]["tools"]}
        assert names == {"backend-a.echo", "backend-b.add"}
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tools_call_via_http() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
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
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
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
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
            resp = await client.post("/mcp", content="qualquer coisa")
        assert resp.status_code == 415
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_body_nao_dict_retorna_invalid_request() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
            resp = await client.post("/mcp", json=[1, 2, 3])  # batch não suportado
        assert resp.status_code == 200
        assert resp.json()["error"]["code"] == -32600
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_notificacao_retorna_202_sem_corpo() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
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
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
            resp = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 7})  # sem method
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["error"]["code"] == -32600  # InvalidRequest
        assert payload["id"] == 7
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_payload_grande_demais_retorna_413() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server, max_payload_bytes=100)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
            resp = await client.post(
                "/mcp",
                content="x" * 500,
                headers={"content-type": "application/json"},
            )
        assert resp.status_code == 413
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_auth_401_quando_token_esperado_e_header_ausente() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
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
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
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
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
            resp = await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                headers={"Authorization": "Bearer segredo"},
            )
        assert resp.status_code == 200
        assert resp.json().get("error") is None
    finally:
        await server.stop()


# ----------------------------------------------------------------------
# Autenticação via query string (?token=) no POST /mcp — correção do
# escaping quebrado no Windows (cmd.exe corrompe headers com espaço em
# argumentos de npx/mcp-remote).
# ----------------------------------------------------------------------


async def _post_tools_list(client: httpx.AsyncClient, **kwargs: Any) -> httpx.Response:
    return await client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, **kwargs)


@pytest.mark.asyncio
async def test_auth_ok_com_token_via_query_string() -> None:
    """?token= correto, sem header nenhum -> sucesso (novidade da correção)."""
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
            resp = await _post_tools_list(client, params={"token": "segredo"})
        assert resp.status_code == 200
        payload = resp.json()
        assert payload.get("error") is None
        assert {t["name"] for t in payload["result"]["tools"]} == {"backend-a.echo", "backend-b.add"}
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_auth_401_com_token_errado_via_query_string() -> None:
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
            resp = await _post_tools_list(client, params={"token": "errado"})
        assert resp.status_code == 401
        assert resp.headers["www-authenticate"] == "Bearer"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_auth_ok_com_header_errado_e_query_correta() -> None:
    """Ambas as formas enviadas, só a query correta -> aceito (basta UMA
    forma bater; documentado no README como decisão de precedência)."""
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
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
    """Recíproco: header correto + query errada -> aceito (o header correto
    já autentica; a query errada não derruba o acesso)."""
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
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
    do structlog — nem em request aceita, nem em 401 (regressão de vazamento)."""
    server = await make_app_with_fakes()
    try:
        app = create_app(server, auth_token="segredo-secreto")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
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
async def test_request_id_consistente_nos_logs_da_mesma_request(captured_logs: list[dict[str, Any]]) -> None:
    """Todo log de uma request HTTP carrega o mesmo request_id (contextvars)."""
    server = await make_app_with_fakes()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
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
        # Duas requisições distintas -> dois request_ids distintos e consistentes.
        assert len(groups) == 2
        for request_events in groups.values():
            names = {e["event"] for e in request_events}
            assert "jsonrpc_request_received" in names
            assert "http_request_completed" in names
        # A tools/call gerou log de roteamento com o backend escolhido.
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
    """Critério de aceite da Fase 0+1: tools, resources e prompts de 2 backends."""
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
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
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

            # resources/list agrega os dois backends com URI namespaced.
            resp = await client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 3, "method": "resources/list"}
            )
            assert resp.status_code == 200
            uris = {r["uri"] for r in resp.json()["result"]["resources"] if isinstance(r, dict)}
            assert "backend-a.memory://greeting" in uris
            assert "backend-b.file:///tmp/fake-note.txt" in uris

            # resources/read com a URI namespaced roteia para o backend certo.
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

            # prompts/list + prompts/get com namespace.
            resp = await client.post(
                "/mcp", json={"jsonrpc": "2.0", "id": 5, "method": "prompts/list"}
            )
            assert resp.status_code == 200
            prompt_names = {p["name"] for p in resp.json()["result"]["prompts"] if isinstance(p, dict)}
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


# ----------------------------------------------------------------------
# Fase 2: rotas de observabilidade GET /health e GET /api/servers
# ----------------------------------------------------------------------


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
    manager, registries = make_manager_for_clients(
        {"backend-a": client_a, "backend-b": client_b}
    )
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

            resp = await client.get(
                "/api/servers", headers={"Authorization": "Bearer segredo"}
            )
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