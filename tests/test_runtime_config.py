"""Testes de configuração runtime do processo do Gateway."""

import pytest
from pydantic import ValidationError

from gateway.config import BackendConfig, GatewayConfig
from gateway import __version__
from gateway.clients.base import BaseClient, PROTOCOL_VERSION as CLIENT_PROTOCOL_VERSION
from gateway.errors import BackendError
from gateway.http_server import APP_VERSION, _dashboard_browser_url
from gateway.models import PROTOCOL_VERSION
from gateway.server import SERVER_VERSION, PROTOCOL_VERSION as SERVER_PROTOCOL_VERSION
from main import _configured_host


def test_versao_runtime_e_centralizada() -> None:
    assert APP_VERSION == __version__
    assert SERVER_VERSION == __version__
    assert APP_VERSION == "0.5.0"


def test_protocol_version_tem_fonte_unica() -> None:
    assert SERVER_PROTOCOL_VERSION is PROTOCOL_VERSION
    assert CLIENT_PROTOCOL_VERSION is PROTOCOL_VERSION


@pytest.mark.asyncio
async def test_handshake_do_client_anuncia_versao_do_app() -> None:
    class CaptureClient(BaseClient):
        def __init__(self) -> None:
            # Precisa inicializar o estado do BaseClient (_initializing,
            # _pending, _state, _capabilities); sem super().__init__() o
            # guard `if self._initializing` em _initialize() estoura AttributeError.
            super().__init__()
            self.request: dict[str, object] | None = None

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def send_request(self, method: str, params=None):
            self.request = {"method": method, "params": params}
            return {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
            }

    client = CaptureClient()
    await client._initialize()
    assert client.request is not None
    params = client.request["params"]
    assert isinstance(params, dict)
    assert params["clientInfo"] == {"name": "mcp-gateway", "version": __version__}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        {},
        {"protocolVersion": PROTOCOL_VERSION},
        {"protocolVersion": PROTOCOL_VERSION, "capabilities": []},
        {"protocolVersion": "unsupported", "capabilities": {}},
    ],
)
async def test_handshake_rejeita_resposta_invalida_do_backend(result) -> None:
    class InvalidResponseClient(BaseClient):
        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def send_request(self, method: str, params=None):
            return result

    with pytest.raises(BackendError):
        await InvalidResponseClient()._initialize()


def test_host_padrao_local_e_override_por_ambiente(monkeypatch) -> None:
    monkeypatch.delenv("MCP_GATEWAY_HOST", raising=False)
    assert _configured_host() == "127.0.0.1"

    monkeypatch.setenv("MCP_GATEWAY_HOST", "0.0.0.0")
    assert _configured_host() == "0.0.0.0"

    monkeypatch.setenv("MCP_GATEWAY_HOST", "   ")
    assert _configured_host() == "127.0.0.1"


@pytest.mark.parametrize(
    ("host", "porta", "esperado"),
    [
        (None, "8080", "http://127.0.0.1:8080/"),
        ("192.168.1.10", "8080", "http://192.168.1.10:8080/"),
        ("0.0.0.0", "8080", "http://127.0.0.1:8080/"),  # navegável no loopback
        ("::1", "8080", "http://[::1]:8080/"),
        ("   ", "9000", "http://127.0.0.1:9000/"),
    ],
)
def test_url_do_auto_open_deriva_do_host_do_bind(
    monkeypatch: pytest.MonkeyPatch, host: str | None, porta: str, esperado: str
) -> None:
    """A URL do auto-open usa o MESMO host (e normalização) do bind do main.py.

    Regressão: a URL era fixa em 127.0.0.1, ignorando ``MCP_GATEWAY_HOST`` —
    bind em IP específico abria o navegador no endereço errado. "0.0.0.0"
    é mapeado para 127.0.0.1 de propósito (não é endereço navegável).
    """
    monkeypatch.delenv("MCP_GATEWAY_HOST", raising=False)
    monkeypatch.delenv("MCP_GATEWAY_PORT", raising=False)
    if host is not None:
        monkeypatch.setenv("MCP_GATEWAY_HOST", host)
    monkeypatch.setenv("MCP_GATEWAY_PORT", porta)
    assert _dashboard_browser_url(auth_token=None) == esperado


def test_url_do_auto_open_embute_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MCP_GATEWAY_HOST", raising=False)
    monkeypatch.setenv("MCP_GATEWAY_PORT", "8080")
    url = _dashboard_browser_url(auth_token="segredo")
    assert url == "http://127.0.0.1:8080/?token=segredo"


def test_auth_token_vazio_rejeitado_pela_validacao_do_campo() -> None:
    with pytest.raises(ValidationError, match="String should have at least 1 character"):
        GatewayConfig(backends=[BackendConfig(name="a", command="x")], auth_token="")
