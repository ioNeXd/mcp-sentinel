"""Testes de configuração runtime do processo do Gateway."""  
  
import pytest  
from pydantic import ValidationError  
  
from gateway.config import BackendConfig, GatewayConfig  
from gateway import __version__  
from gateway.clients.base import BaseClient, PROTOCOL_VERSION as CLIENT_PROTOCOL_VERSION  
from gateway.errors import BackendError  
from gateway.http_server import APP_VERSION  
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
  
  
def test_auth_token_vazio_rejeitado_pela_validacao_do_campo() -> None:  
    with pytest.raises(ValidationError, match="String should have at least 1 character"):  
        GatewayConfig(backends=[BackendConfig(name="a", command="x")], auth_token="")