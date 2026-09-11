"""Testes HTTP das rotas de controle de backends e do dashboard (Fase 4)."""

import pytest
import httpx

from conftest import FakeClient, make_manager_for_clients
from gateway.backend_manager import BackendStatus
from gateway.errors import BackendError
from gateway.http_server import create_app
from gateway.server import McpServer

ECHO_TOOL = {
    "name": "echo",
    "description": "Repete texto.",
    "inputSchema": {"type": "object", "properties": {}},
}

BASE_URL = "http://test"


async def make_app() -> McpServer:
    client_a = FakeClient(tools=[ECHO_TOOL])
    client_b = FakeClient(tools=[dict(ECHO_TOOL, name="add")])
    manager, registries = make_manager_for_clients({"backend-a": client_a, "backend-b": client_b})
    server = McpServer(manager, registries)
    await server.start()
    return server


async def post_control(app: object, path: str, token: str | None = None) -> httpx.Response:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=BASE_URL
    ) as client:
        return await client.post(path, headers=headers)


# ----------------------------------------------------------------------
# disable / enable / restart
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disable_via_http() -> None:
    server = await make_app()
    try:
        app = create_app(server)
        resp = await post_control(app, "/api/servers/backend-a/disable")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["backend"] == "backend-a"
        assert payload["action"] == "disable"
        assert payload["status"] == "disabled"
        # Estado visível nas rotas de observabilidade.
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            health = (await client.get("/health")).json()
            assert health["backends"]["disabled"] == 1
            assert health["status"] == "ok"  # disabled não degrada
            servers = {s["name"]: s for s in (await client.get("/api/servers")).json()["servers"]}
            assert servers["backend-a"]["status"] == "disabled"
            assert servers["backend-a"]["tools_count"] == 0
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_disable_entao_enable_via_http() -> None:
    server = await make_app()
    try:
        app = create_app(server)
        await post_control(app, "/api/servers/backend-a/disable")
        resp = await post_control(app, "/api/servers/backend-a/enable")
        assert resp.status_code == 200
        assert resp.json()["status"] == "running"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            servers = {s["name"]: s for s in (await client.get("/api/servers")).json()["servers"]}
            assert servers["backend-a"]["tools_count"] == 1  # tools de volta
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_restart_via_http_em_backend_running() -> None:
    """Restart manual de um backend saudável via API — sem ter caído antes."""
    server = await make_app()
    try:
        app = create_app(server)
        resp = await post_control(app, "/api/servers/backend-a/restart")
        assert resp.status_code == 200
        assert resp.json()["status"] == "running"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_rotas_de_controle_404_para_backend_inexistente() -> None:
    server = await make_app()
    try:
        app = create_app(server)
        for action in ("disable", "enable", "restart"):
            resp = await post_control(app, f"/api/servers/fantasma/{action}")
            assert resp.status_code == 404, action
            assert "não existe" in resp.json()["detail"]
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_enable_em_backend_nao_disabled_retorna_409() -> None:
    server = await make_app()
    try:
        app = create_app(server)
        resp = await post_control(app, "/api/servers/backend-a/enable")
        assert resp.status_code == 409  # running: enable não se aplica
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_mensagem_semantica_nao_classifica_backend_error_por_substring() -> None:
    server = await make_app()
    try:
        app = create_app(server)

        async def renamed_error(name: str) -> None:
            raise BackendError("backend não existe no config, mas erro genérico")

        server.backend_manager.disable = renamed_error  # type: ignore[method-assign]
        resp = await post_control(app, "/api/servers/backend-a/disable")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "backend não existe no config, mas erro genérico"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_restart_falhando_retorna_503_sem_derrubar_gateway() -> None:
    """Falha de subida no restart vira 503; o Gateway continua de pé."""
    server = await make_app()
    try:
        app = create_app(server)
        manager = server.backend_manager

        from conftest import FakeClient as FC

        def broken_factory(backend_config: object) -> FC:
            return FC(start_error=True)

        manager._create_client = broken_factory  # type: ignore[method-assign]
        resp = await post_control(app, "/api/servers/backend-a/restart")
        assert resp.status_code == 503
        assert "não subiu" in resp.json()["detail"]

        # O Gateway inteiro continua respondendo.
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            assert (await client.get("/health")).status_code == 200
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_rotas_de_controle_exigem_auth_mesmo_com_token_configurado() -> None:
    server = await make_app()
    try:
        app = create_app(server, auth_token="segredo")
        for action in ("disable", "enable", "restart"):
            resp = await post_control(app, f"/api/servers/backend-a/{action}")
            assert resp.status_code == 401, action
            assert resp.headers["www-authenticate"] == "Bearer"
        # Backend intacto: nada foi executado sem auth.
        assert server.backend_manager.status_of("backend-a") is BackendStatus.RUNNING
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_rotas_de_controle_com_token_correto_funcionam() -> None:
    server = await make_app()
    try:
        app = create_app(server, auth_token="segredo")
        resp = await post_control(app, "/api/servers/backend-a/disable", token="segredo")
        assert resp.status_code == 200
        assert resp.json()["status"] == "disabled"
        resp = await post_control(app, "/api/servers/backend-a/enable", token="segredo")
        assert resp.status_code == 200
        assert resp.json()["status"] == "running"
    finally:
        await server.stop()


# ----------------------------------------------------------------------
# Dashboard GET /
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dashboard_html_sem_auth() -> None:
    """GET / devolve HTML com status e nomes dos backends (server-rendered)."""
    server = await make_app()
    try:
        app = create_app(server)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.get("/")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/html")
        body = resp.text
        assert "<!DOCTYPE html>" in body
        assert "backend-a" in body  # nomes dos backends visíveis
        assert "backend-b" in body
        assert "echo" in body or "running" in body  # status/tipo presentes
        assert "stdio" in body  # coluna de tipo
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_dashboard_reflete_estado_disabled() -> None:
    server = await make_app()
    try:
        app = create_app(server)
        await post_control(app, "/api/servers/backend-a/disable")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            body = (await client.get("/")).text
        assert "disabled" in body  # status do backend-a aparece na tabela
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_dashboard_com_auth_exige_token() -> None:
    server = await make_app()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.get("/")
            assert resp.status_code == 401  # sem token nenhum

            resp = await client.get("/", params={"token": "segredo"})  # ?token=
            assert resp.status_code == 200
            assert "backend-a" in resp.text

            resp = await client.get("/", params={"token": "errado"})
            assert resp.status_code == 401

            # Header Bearer também vale (mesma auth do /mcp).
            resp = await client.get("/", headers={"Authorization": "Bearer segredo"})
            assert resp.status_code == 200
    finally:
        await server.stop()
