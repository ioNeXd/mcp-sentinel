"""Testes do BackendType e da validação de backends por transporte (Fase 3)."""

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from gateway.config import (
    BackendConfig,
    BackendType,
    GatewayConfig,
    load_config,
    resolve_config_path,
)


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


def test_config_com_zero_backends_e_rejeitado() -> None:
    """O schema EXIGE ao menos um backend (model_validator da GatewayConfig).

    Regressão da verificação do item 24: o campo ``backends`` não tem
    ``min_length=1`` na declaração, mas o ``model_validator`` da classe
    rejeita lista vazia — o aviso do importador ("o Gateway exige ao menos
    um backend") está correto e o boot de um config vazio falha cedo.
    """
    with pytest.raises(ValidationError, match="ao menos um backend"):
        GatewayConfig(backends=[])


def test_load_config_local_sobrescreve_auth_token(tmp_path) -> None:
    """O config.local.json ao lado sobrescreve chaves de topo (auth_token fora do repo)."""
    (tmp_path / "config.json").write_text(
        '{"backends": [{"name": "a", "command": "python"}], "auth_token": null}',
        encoding="utf-8",
    )
    (tmp_path / "config.local.json").write_text(
        '{"auth_token": "segredo-local"}',
        encoding="utf-8",
    )
    config = load_config(tmp_path / "config.json")
    assert config.auth_token == "segredo-local"


def test_load_config_sem_local_comportamento_identico(tmp_path) -> None:
    """Sem config.local.json o arquivo principal vale inteiro (regressão)."""
    (tmp_path / "config.json").write_text(
        '{"backends": [{"name": "a", "command": "python"}], "auth_token": null,'
        ' "session_ttl_seconds": 600}',
        encoding="utf-8",
    )
    config = load_config(tmp_path / "config.json")
    assert config.auth_token is None
    assert config.session_ttl_seconds == 600


def test_load_config_local_invalido_value_error(tmp_path) -> None:
    """JSON malformado no local vira ValueError apontando o arquivo local."""
    (tmp_path / "config.json").write_text(
        '{"backends": [{"name": "a", "command": "python"}]}',
        encoding="utf-8",
    )
    (tmp_path / "config.local.json").write_text("{quebrado", encoding="utf-8")
    with pytest.raises(ValueError, match="config.local.json"):
        load_config(tmp_path / "config.json")


def test_load_config_local_backends_vencem(tmp_path) -> None:
    """backends do local substituem a lista inteira (nao faz merge por nome)."""
    (tmp_path / "config.json").write_text(
        '{"backends": [{"name": "a", "command": "python"},'
        ' {"name": "b", "command": "python"}]}',
        encoding="utf-8",
    )
    (tmp_path / "config.local.json").write_text(
        '{"backends": [{"name": "so-local", "command": "python"}]}',
        encoding="utf-8",
    )
    config = load_config(tmp_path / "config.json")
    assert [b.name for b in config.backends] == ["so-local"]


class TestResolveConfigPath:
    """CLI pip: env vence, cwd do repo vence, senão cria ~/.sentinel/config.json."""

    def test_env_vence_sobre_cwd_e_home(self, tmp_path, monkeypatch) -> None:
        """MCP_GATEWAY_CONFIG explícito retorna como está (load_config reporta se faltar)."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "config").mkdir()
        (tmp_path / "config" / "config.json").write_text('{"backends": []}', encoding="utf-8")
        env = tmp_path / "custom" / "cfg.json"
        assert resolve_config_path(str(env), home=tmp_path / "home") == env

    def test_cwd_do_repo_vence_sobre_home(self, tmp_path, monkeypatch) -> None:
        """Fluxo dev: config/config.json no cwd é usado sem tocar no ~/.sentinel."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "config").mkdir()
        repo_cfg = tmp_path / "config" / "config.json"
        repo_cfg.write_text('{"backends": []}', encoding="utf-8")
        home = tmp_path / "home"
        assert resolve_config_path(None, home=home) == Path("config/config.json")
        assert not home.exists()

    def test_primeira_execucao_cria_config_valido(self, tmp_path, monkeypatch) -> None:
        """Sem cwd config: cria ~/.sentinel/config.json com o backend sample pronto."""
        monkeypatch.chdir(tmp_path)  # cwd sem config/config.json
        home = tmp_path / "home"
        path = resolve_config_path(None, home=home)
        assert path == home / ".sentinel" / "config.json"
        assert path.is_file()
        config = load_config(path)
        assert len(config.backends) == 1
        sample = config.backends[0]
        assert sample.name == "sample"
        assert sample.command == sys.executable
        assert sample.args == ["-m", "gateway.sample_backend"]

    def test_segunda_chamada_nao_sobrescreve(self, tmp_path, monkeypatch) -> None:
        """Resolução é idempotente: edits do usuário no config nunca são recriados por cima."""
        monkeypatch.chdir(tmp_path)
        home = tmp_path / "home"
        path = resolve_config_path(None, home=home)
        path.write_text(
            '{"backends": [{"name": "meu", "command": "python"}],'
            ' "max_payload_bytes": 1024}',
            encoding="utf-8",
        )
        assert resolve_config_path(None, home=home) == path
        config = load_config(path)
        assert config.backends[0].name == "meu"
        assert config.max_payload_bytes == 1024
