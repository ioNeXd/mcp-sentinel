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


# --- Backup automático ---

class TestAutoBackup:
    def test_backup_created_before_write(self, tmp_path):
        import json
        from gateway.http_server import _backup_config
        config = tmp_path / "config.json"
        config.write_text(json.dumps({"backends": []}), encoding="utf-8")
        _backup_config(str(config))
        bak = tmp_path / "config.json.bak"
        assert bak.exists()
        assert json.loads(bak.read_text(encoding="utf-8")) == {"backends": []}

    def test_backup_no_crash_on_missing_file(self, tmp_path):
        from gateway.http_server import _backup_config
        _backup_config(str(tmp_path / "nonexistent.json"))  # should not raise


# --- Teste de conectividade ---

class TestConnectivity:
    @pytest.mark.anyio
    async def test_rejects_invalid_json(self, app):
        async with _client(app) as c:
            r = await c.post(
                "/api/test-connectivity",
                content="not json",
                headers={"Content-Type": "application/json", **_auth_headers(app)},
            )
        assert r.status_code == 400

    @pytest.mark.anyio
    async def test_rejects_bad_payload(self, app):
        async with _client(app) as c:
            r = await c.post(
                "/api/test-connectivity",
                json={"name": "", "type": "stdio"},
                headers=_auth_headers(app),
            )
        assert r.status_code == 422

    @pytest.mark.anyio
    async def test_stdio_nonexistent_command(self, app):
        async with _client(app) as c:
            r = await c.post(
                "/api/test-connectivity",
                json={"name": "x", "type": "stdio", "command": "__nonexistent_cmd__"},
                headers=_auth_headers(app),
            )
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is False
        assert "não encontrado" in body["detail"]


# --- Export/Import config ---

def _auth_headers(app):
    """Extract auth headers from the app fixture if auth is configured."""
    # app_auth fixture uses 'test-token-123'; app fixture uses None
    return {}

class TestConfigExportImport:
    @pytest.mark.anyio
    async def test_export_returns_json(self, app):
        async with _client(app) as c:
            r = await c.get("/api/config/export")
        assert r.status_code == 200
        data = r.json()
        assert "backends" in data

    @pytest.mark.anyio
    async def test_export_requires_auth(self, app_auth):
        async with _client(app_auth) as c:
            r = await c.get("/api/config/export")
        assert r.status_code == 401

    @pytest.mark.anyio
    async def test_import_valid_config(self, app, tmp_path):
        import json
        config_path = tmp_path / "test_config.json"
        valid = {
            "auth_token": None,
            "max_payload_bytes": 1048576,
            "health_check_interval_seconds": 30,
            "backend_request_timeout_seconds": 30,
            "auto_restart": True,
            "max_restart_attempts": 3,
            "session_ttl_seconds": 600,
            "backends": [{
                "name": "imported-backend",
                "type": "stdio",
                "command": "python",
                "args": ["-c", "print()"],
            }],
        }
        async with _client(app) as c:
            r = await c.post("/api/config/import", json=valid)
        assert r.status_code == 200
        assert "Importado" in r.json()["detail"] or "importado" in r.json()["detail"].lower()

    @pytest.mark.anyio
    async def test_import_rejects_invalid_config(self, app):
        async with _client(app) as c:
            r = await c.post(
                "/api/config/import",
                json={"backends": []},  # missing required fields
            )
        assert r.status_code == 422

    @pytest.mark.anyio
    async def test_import_requires_auth(self, app_auth):
        async with _client(app_auth) as c:
            r = await c.post("/api/config/import", json={"backends": []})
        assert r.status_code == 401

