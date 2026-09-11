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
    manager, registries = make_manager_for_clients(  
        {"backend-a": client_a, "backend-b": client_b}  
    )  
    server = McpServer(manager, registries)  
    await server.start()  
    return server  
  
  
async def post_control(  
    app: object, path: str, token: str | None = None  
) -> httpx.Response:  
    headers = {"Authorization": f"Bearer {token}"} if token else {}  
    async with httpx.AsyncClient(  
        transport=httpx.ASGITransport(app=app), base_url=BASE_URL  
    ) as client:  
        return await client.post(path, headers=headers)  
  
  
@pytest.mark.asyncio  
async def test_disable_via_http() -> None:  
    """disable via HTTP: 200 com payload de ação e estado visível na observabilidade.  
  
    Após o disable, /health conta o backend como disabled (sem degradar o  
    status geral) e /api/servers reporta status=disabled com tools_count zerado.  
    """  
    server = await make_app()  
    try:  
        app = create_app(server)  
        resp = await post_control(app, "/api/servers/backend-a/disable")  
        assert resp.status_code == 200  
        payload = resp.json()  
        assert payload["backend"] == "backend-a"  
        assert payload["action"] == "disable"  
        assert payload["status"] == "disabled"  
        async with httpx.AsyncClient(  
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL  
        ) as client:  
            health = (await client.get("/health")).json()  
            assert health["backends"]["disabled"] == 1  
            assert health["status"] == "ok"  
            servers = {s["name"]: s for s in (await client.get("/api/servers")).json()["servers"]}  
            assert servers["backend-a"]["status"] == "disabled"  
            assert servers["backend-a"]["tools_count"] == 0  
    finally:  
        await server.stop()  
  
  
@pytest.mark.asyncio  
async def test_disable_entao_enable_via_http() -> None:  
    """Ciclo disable→enable via HTTP restaura o backend e reexpõe suas tools."""  
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
            assert servers["backend-a"]["tools_count"] == 1  
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
    """Backend fora do config: disable/enable/restart devolvem 404 com detalhe claro."""  
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
    """enable num backend RUNNING é estado conflitante: 409 (enable não se aplica)."""  
    server = await make_app()  
    try:  
        app = create_app(server)  
        resp = await post_control(app, "/api/servers/backend-a/enable")  
        assert resp.status_code == 409  
    finally:  
        await server.stop()  
  
  
@pytest.mark.asyncio  
async def test_mensagem_semantica_nao_classifica_backend_error_por_substring() -> None:  
    """BackendError genérico vira 503, não 404 por conter 'não existe' na mensagem.  
  
    A classificação do status HTTP é por tipo/semântica do erro, nunca por  
    casamento de substring na mensagem (que poderia mascarar um erro genérico  
    como 'inexistente').  
    """  
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
  
        async with httpx.AsyncClient(  
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL  
        ) as client:  
            assert (await client.get("/health")).status_code == 200  
    finally:  
        await server.stop()  
  
  
@pytest.mark.asyncio  
async def test_rotas_de_controle_exigem_auth_mesmo_com_token_configurado() -> None:  
    """Com token configurado, disable/enable/restart sem auth devolvem 401 e não executam nada."""  
    server = await make_app()  
    try:  
        app = create_app(server, auth_token="segredo")  
        for action in ("disable", "enable", "restart"):  
            resp = await post_control(app, f"/api/servers/backend-a/{action}")  
            assert resp.status_code == 401, action  
            assert resp.headers["www-authenticate"] == "Bearer"  
        assert server.backend_manager.status_of("backend-a") is BackendStatus.RUNNING  
    finally:  
        await server.stop()  
  
  
@pytest.mark.asyncio  
async def test_rotas_de_controle_com_token_correto_funcionam() -> None:  
    """Com o token correto no header, disable e enable executam normalmente."""  
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
        assert "backend-a" in body  
        assert "backend-b" in body  
        assert "echo" in body or "running" in body  
        assert "stdio" in body  
    finally:  
        await server.stop()  
  
  
@pytest.mark.asyncio  
async def test_dashboard_reflete_estado_disabled() -> None:  
    """Após disable, o status 'disabled' do backend aparece renderizado no dashboard."""  
    server = await make_app()  
    try:  
        app = create_app(server)  
        await post_control(app, "/api/servers/backend-a/disable")  
        async with httpx.AsyncClient(  
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL  
        ) as client:  
            body = (await client.get("/")).text  
        assert "disabled" in body  
    finally:  
        await server.stop()  
  
  
@pytest.mark.asyncio  
async def test_dashboard_com_auth_exige_token() -> None:  
    """Dashboard com auth: 401 sem token; ?token= e header Bearer (mesma auth do /mcp) liberam."""  
    server = await make_app()  
    try:  
        app = create_app(server, auth_token="segredo")  
        async with httpx.AsyncClient(  
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL  
        ) as client:  
            resp = await client.get("/")  
            assert resp.status_code == 401  
  
            resp = await client.get("/", params={"token": "segredo"})  
            assert resp.status_code == 200  
            assert "backend-a" in resp.text  
  
            resp = await client.get("/", params={"token": "errado"})  
            assert resp.status_code == 401  
  
            resp = await client.get("/", headers={"Authorization": "Bearer segredo"})  
            assert resp.status_code == 200  
    finally:  
        await server.stop()