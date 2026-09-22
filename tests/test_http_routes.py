"""Testes de integração HTTP do Gateway (rotas REST + health).

Usa ``httpx.AsyncClient`` com ``ASGITransport`` para testar o app FastAPI
sem subir uvicorn. Cada teste cria um ``McpServer`` mockado com mínimo
necessário (sem backends reais).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from gateway.backend_manager import BackendManager
from gateway.config import BackendConfig, BackendType, GatewayConfig
from gateway.http_server import create_app
from gateway.registries import PromptRegistry, ResourceRegistry, ToolRegistry


_DUMMY_BACKEND = BackendConfig(
    name="__test__",
    type=BackendType.STDIO,
    command="python",
    args=["-c", "print()"],
)


def _make_config(auth_token: str | None = None) -> GatewayConfig:
    return GatewayConfig(backends=[_DUMMY_BACKEND], auth_token=auth_token)


def _make_mcp_server(config: GatewayConfig | None = None) -> MagicMock:
    config = config or _make_config()
    registries = (ToolRegistry(), ResourceRegistry(), PromptRegistry())
    bm = BackendManager(config, registries)
    server = MagicMock()
    server.backend_manager = bm
    server.process_message = AsyncMock(return_value={})
    server.tools_list_size = MagicMock(return_value={"chars": 0, "tokens": 0})
    return server


@pytest.fixture
def app():
    return create_app(_make_mcp_server())


@pytest.fixture
def app_auth():
    return create_app(_make_mcp_server(_make_config("test-token-123")), auth_token="test-token-123")


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


# --- Health ---


class TestHealth:
    @pytest.mark.anyio
    async def test_health_no_auth(self, app):
        async with _client(app) as c:
            r = await c.get("/health")
        assert r.status_code == 200
        assert "status" in r.json()

    @pytest.mark.anyio
    async def test_health_always_accessible(self, app_auth):
        async with _client(app_auth) as c:
            r = await c.get("/health")
        assert r.status_code == 200


# --- Auth ---


class TestAuth:
    @pytest.mark.anyio
    async def test_servers_without_token_returns_401(self, app_auth):
        async with _client(app_auth) as c:
            r = await c.get("/api/servers")
        assert r.status_code == 401

    @pytest.mark.anyio
    async def test_servers_with_valid_token(self, app_auth):
        async with _client(app_auth) as c:
            r = await c.get("/api/servers", headers={"Authorization": "Bearer test-token-123"})
        assert r.status_code == 200
        assert "servers" in r.json()

    @pytest.mark.anyio
    async def test_servers_with_wrong_token_returns_401(self, app_auth):
        async with _client(app_auth) as c:
            r = await c.get("/api/servers", headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 401

    @pytest.mark.anyio
    async def test_servers_no_auth_always_ok(self, app):
        async with _client(app) as c:
            r = await c.get("/api/servers")
        assert r.status_code == 200


# --- Dashboard ---


class TestDashboard:
    @pytest.mark.anyio
    async def test_dashboard_returns_html(self, app):
        async with _client(app) as c:
            r = await c.get("/")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]

    @pytest.mark.anyio
    async def test_dashboard_no_auth_returns_401(self, app_auth):
        async with _client(app_auth) as c:
            r = await c.get("/")
        assert r.status_code == 401


# --- Tools size ---


class TestToolsSize:
    @pytest.mark.anyio
    async def test_tools_size(self, app):
        async with _client(app) as c:
            r = await c.get("/api/tools/size")
        assert r.status_code == 200


# --- Shutdown ---


class TestShutdown:
    @pytest.mark.anyio
    async def test_shutdown_without_server_returns_501(self, app):
        async with _client(app) as c:
            r = await c.post("/api/shutdown")
        assert r.status_code == 501

    @pytest.mark.anyio
    async def test_shutdown_with_mock_server(self, app):
        mock_server = MagicMock()
        mock_server.should_exit = False
        app.state.uvicorn_server = mock_server
        async with _client(app) as c:
            r = await c.post("/api/shutdown")
        assert r.status_code == 200
        assert "Encerrando" in r.json()["detail"]


# --- Rate limiting ---


class TestRateLimit:
    def test_rate_limiter_allows_within_window(self):
        from gateway.rate_limiter import RateLimiter
        rl = RateLimiter(max_requests=3, window_seconds=60)
        assert rl.allow("a") is True
        assert rl.allow("a") is True
        assert rl.allow("a") is True
        assert rl.allow("a") is False

    def test_rate_limiter_separate_keys(self):
        from gateway.rate_limiter import RateLimiter
        rl = RateLimiter(max_requests=1, window_seconds=60)
        assert rl.allow("a") is True
        assert rl.allow("b") is True
        assert rl.allow("a") is False
