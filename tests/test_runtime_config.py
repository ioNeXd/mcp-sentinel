"""Testes de configuração runtime do processo do Gateway."""

import pytest
from pydantic import ValidationError

from gateway.config import BackendConfig, GatewayConfig
from gateway.http_server import APP_VERSION
from gateway.server import SERVER_VERSION
from gateway.version import __version__
from main import _configured_host


def test_versao_runtime_e_centralizada() -> None:
    assert APP_VERSION == __version__
    assert SERVER_VERSION == __version__


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
