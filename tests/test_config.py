"""Testes do BackendType e da validação de backends por transporte (Fase 3)."""

import pytest
from pydantic import ValidationError

from gateway.config import BackendConfig, BackendType, GatewayConfig, load_config


class TestBackendTypeValidation:
    """command/args obrigatórios só no stdio; url obrigatória em http/sse."""

    def test_stdio_com_command_ok(self) -> None:
        config = BackendConfig(name="a", command="python", args=["x.py"])
        assert config.type is BackendType.STDIO

    def test_stdio_sem_command_erro_claro(self) -> None:
        with pytest.raises(ValidationError, match="command.*obrigatório"):
            BackendConfig(name="a", type="stdio")

    def test_stdio_com_url_erro(self) -> None:
        with pytest.raises(ValidationError, match="url.*não se aplica"):
            BackendConfig(name="a", command="python", url="http://x")

    def test_http_sem_url_erro(self) -> None:
        with pytest.raises(ValidationError, match="url.*obrigatório"):
            BackendConfig(name="a", type="http")

    def test_http_com_command_erro(self) -> None:
        with pytest.raises(ValidationError, match="só se aplicam a backends stdio"):
            BackendConfig(name="a", type="http", url="http://x", command="python")

    def test_http_com_args_erro(self) -> None:
        with pytest.raises(ValidationError, match="só se aplicam a backends stdio"):
            BackendConfig(name="a", type="http", url="http://x", args=["x"])

    def test_sse_sem_url_erro(self) -> None:
        with pytest.raises(ValidationError, match="url.*obrigatório"):
            BackendConfig(name="a", type="sse")

    def test_http_com_url_e_headers_ok(self) -> None:
        config = BackendConfig(
            name="remoto",
            type="http",
            url="http://127.0.0.1:9000",
            headers={"Authorization": "Bearer xyz"},
        )
        assert config.type is BackendType.HTTP
        assert config.headers["Authorization"] == "Bearer xyz"

    def test_type_invalido_rejeitado(self) -> None:
        with pytest.raises(ValidationError):
            BackendConfig(name="a", type="websocket", url="ws://x")

    def test_type_ausente_default_stdio(self) -> None:
        """Config da Fase 0/1 sem campo type continua válido (compatibilidade)."""
        config = BackendConfig(name="a", command="python")
        assert config.type is BackendType.STDIO

    def test_timeout_por_backend_invalido(self) -> None:
        with pytest.raises(ValidationError):
            BackendConfig(name="a", type="http", url="http://x", request_timeout_seconds=0)

class TestGatewayRequestTimeout:
    """Resolução do timeout: específico do backend vence o global."""

    def test_backend_especifico_vence_global(self) -> None:
        gateway = GatewayConfig(
            backends=[BackendConfig(name="a", command="x", request_timeout_seconds=3.0)],
            backend_request_timeout_seconds=10.0,
        )
        assert gateway.request_timeout_for(gateway.backends[0]) == 3.0

    def test_global_usado_quando_backend_omito(self) -> None:
        gateway = GatewayConfig(
            backends=[BackendConfig(name="a", command="x")],
            backend_request_timeout_seconds=10.0,
        )
        assert gateway.request_timeout_for(gateway.backends[0]) == 10.0

    def test_default_quando_nenhum_configurado(self) -> None:
        gateway = GatewayConfig(backends=[BackendConfig(name="a", command="x")])
        assert gateway.request_timeout_for(gateway.backends[0]) > 0


def test_load_config_aceita_config_misto(tmp_path) -> None:
    config_file = tmp_path / "config.json"
    config_file.write_text(
        """
{
  "backends": [
    {"name": "local", "command": "python", "args": ["fake.py"]},
    {"name": "remoto", "type": "http", "url": "http://127.0.0.1:9000"},
    {"name": "eventos", "type": "sse", "url": "http://127.0.0.1:9001",
     "headers": {"Authorization": "Bearer tok"}}
  ]
}
""",
        encoding="utf-8",
    )
    config = load_config(config_file)
    assert [b.type for b in config.backends] == [
        BackendType.STDIO,
        BackendType.HTTP,
        BackendType.SSE,
    ]
    assert config.backends[2].headers["Authorization"] == "Bearer tok"


def test_load_config_schema_invalido_levanta_value_error(tmp_path) -> None:
    """JSON sintaticamente válido mas com backend stdio sem command → ValueError.

    O pydantic.ValidationError deve ser convertido em ValueError (não vazar
    cru), com mensagem legível contendo localização do campo.
    """
    config_file = tmp_path / "config.json"
    config_file.write_text(
        """
{
  "backends": [
    {"name": "sem_cmd", "type": "stdio"}
  ]
}
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="command"):
        load_config(config_file)
