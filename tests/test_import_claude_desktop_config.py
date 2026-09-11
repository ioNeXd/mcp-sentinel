"""Testes do importador de ``claude_desktop_config.json`` (Fase 4).  
  
O importador é um script standalone (``scripts/import_claude_desktop_config.py``);  
estes testes exercitam as funções puras (``import_config``, ``convert_entry``,  
``sanitize_backend_name``) e a CLI (via ``main(argv)``) contra fixtures em  
arquivos temporários.  
"""  
  
import importlib.util  
import json  
import sys  
from pathlib import Path  
from typing import Any  
  
import pytest  
  
ROOT = Path(__file__).resolve().parents[1]  
if str(ROOT) not in sys.path:  
    sys.path.insert(0, str(ROOT))  
  
_IMPORTER_PATH = ROOT / "scripts" / "import_claude_desktop_config.py"  
_spec = importlib.util.spec_from_file_location("import_claude_desktop_config", _IMPORTER_PATH)  
assert _spec is not None and _spec.loader is not None  # noqa: S101 — fixture de import  
importer = importlib.util.module_from_spec(_spec)  
_spec.loader.exec_module(importer)  
  
  
def write_source(tmp_path: Path, mcp_servers: dict[str, Any]) -> Path:  
    source = tmp_path / "claude_desktop_config.json"  
    source.write_text(  
        json.dumps({"mcpServers": mcp_servers}, ensure_ascii=False), encoding="utf-8"  
    )  
    return source  
  
  
SIMPLE_STDIO_ENTRY = {  
    "command": "npx",  
    "args": ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],  
}  
  
  
def test_converte_entrada_stdio_simples(tmp_path: Path) -> None:  
    source = write_source(  
        tmp_path,  
        {"filesystem": dict(SIMPLE_STDIO_ENTRY)},  
    )  
    config, warnings = importer.import_config(source)  
    assert config["backends"] == [  
        {  
            "name": "filesystem",  
            "type": "stdio",  
            "command": "npx",  
            "args": ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],  
        }  
    ]  
    assert warnings == []  
  
  
def test_config_gerado_passa_na_validacao_do_gateway(tmp_path: Path) -> None:  
    """O output do importador é um config.json válido para o Gateway."""  
    from gateway.config import GatewayConfig  
  
    source = write_source(  
        tmp_path,  
        {  
            "filesystem": dict(SIMPLE_STDIO_ENTRY),  
            "git": {"command": "uvx", "args": ["mcp-server-git"]},  
        },  
    )  
    config, _ = importer.import_config(source)  
    validated = GatewayConfig.model_validate(config)  # não deve levantar  
    assert [b.name for b in validated.backends] == ["filesystem", "git"]  
    assert all(b.type.value == "stdio" for b in validated.backends)  
  
  
def test_claude_config_sem_mcpServers_levanta_erro(tmp_path: Path) -> None:  
    source = tmp_path / "outro.json"  
    source.write_text(json.dumps({"outra_secao": {}}), encoding="utf-8")  
    with pytest.raises(ValueError, match="mcpServers"):  
        importer.import_config(source)  
  
  
def test_arquivo_inexistente_levanta_erro(tmp_path: Path) -> None:  
    with pytest.raises(ValueError, match="não encontrado"):  
        importer.import_config(tmp_path / "fantasma.json")  
  
  
def test_ponte_mcp_remote_nao_e_convertida_e_e_sinalizada(tmp_path: Path) -> None:  
    """mcp-remote esconde um servidor HTTP/SSE atrás de um processo stdio."""  
    source = write_source(  
        tmp_path,  
        {  
            "notion": {  
                "command": "npx",  
                "args": ["-y", "mcp-remote", "https://mcp.notion.com/mcp"],  
            }  
        },  
    )  
    config, warnings = importer.import_config(source)  
    assert config["backends"] == []  # NADA convertido  
    assert any("mcp-remote" in w and "não conversível automaticamente" in w for w in warnings)  
  
  
def test_variacoes_de_ponte_sao_detectadas(tmp_path: Path) -> None:  
    """Cada aviso cita a ponte detectada e como declarar o backend nativamente."""  
    source = write_source(  
        tmp_path,  
        {  
            "via-supergateway": {"command": "npx", "args": ["-y", "supergateway", "--url", "http://x"]},  
            "via-proxy": {"command": "python", "args": ["-m", "mcp_proxy", "http://x"]},  
            "via-nome-remoto": {"command": "meu-tunnel-remote.exe", "args": []},  
        },  
    )  
    config, warnings = importer.import_config(source)  
    assert config["backends"] == []  
    bridge_warnings = [w for w in warnings if "não conversível automaticamente" in w]  
    assert len(bridge_warnings) == 3  
    assert all('"type": "http"' in w or '"type": "sse"' in w for w in bridge_warnings)  
  
  
def test_url_sem_type_nao_e_convertida(tmp_path: Path) -> None:  
    source = write_source(tmp_path, {"remoto": {"url": "http://127.0.0.1:9000"}})  
    config, warnings = importer.import_config(source)  
    assert config["backends"] == []  
    assert any("type" in w for w in warnings)  
  
  
def test_url_com_type_e_convertida_diretamente(tmp_path: Path) -> None:  
    source = write_source(  
        tmp_path,  
        {  
            "remoto": {"type": "http", "url": "http://127.0.0.1:9000"},  
            "eventos": {  
                "type": "sse",  
                "url": "http://127.0.0.1:9001",  
                "headers": {"Authorization": "Bearer x"},  
            },  
        },  
    )  
    config, warnings = importer.import_config(source)  
    assert config["backends"] == [  
        {"name": "remoto", "type": "http", "url": "http://127.0.0.1:9000"},  
        {  
            "name": "eventos",  
            "type": "sse",  
            "url": "http://127.0.0.1:9001",  
            "headers": {"Authorization": "Bearer x"},  
        },  
    ]  
    assert warnings == []  
  
  
def test_env_do_claude_converte_com_aviso(tmp_path: Path) -> None:  
    """'env' não é aplicado (Gateway não suporta), mas a entrada É convertida."""  
    source = write_source(  
        tmp_path,  
        {  
            "github": {  
                "command": "npx",  
                "args": ["-y", "@modelcontextprotocol/server-github"],  
                "env": {"GITHUB_TOKEN": "segredo"},  
            }  
        },  
    )  
    config, warnings = importer.import_config(source)  
    assert len(config["backends"]) == 1  # convertido  
    assert config["backends"][0]["command"] == "npx"  
    assert any("env" in w and "não é aplicado" in w for w in warnings)  
  
  
def test_entrada_sem_command_nem_url_e_ignorada(tmp_path: Path) -> None:  
    source = write_source(tmp_path, {"vazio": {"args": ["x"]}})  
    config, warnings = importer.import_config(source)  
    assert config["backends"] == []  
    assert any("ignorada" in w for w in warnings)  
  
  
def test_mix_conversiveis_e_nao_conversiveis(tmp_path: Path) -> None:  
    """Fixture do prompt: mistura de stdio simples + mcp-remote."""  
    source = write_source(  
        tmp_path,  
        {  
            "filesystem": dict(SIMPLE_STDIO_ENTRY),  
            "notion": {  
                "command": "npx",  
                "args": ["-y", "mcp-remote", "https://mcp.notion.com/mcp"],  
            },  
            "git": {"command": "uvx", "args": ["mcp-server-git"]},  
        },  
    )  
    config, warnings = importer.import_config(source)  
    names = [b["name"] for b in config["backends"]]  
    assert names == ["filesystem", "git"]  # notion fora  
    assert len(warnings) == 1  
    assert "notion" in warnings[0]  
  
  
def test_nome_com_pontos_e_espacos_e_sanitizado(tmp_path: Path) -> None:  
    """O nome sanitizado é aceito pela validação do Gateway."""  
    source = write_source(  
        tmp_path,  
        {"meu servidor.fs": dict(SIMPLE_STDIO_ENTRY)},  
    )  
    config, warnings = importer.import_config(source)  
    assert config["backends"][0]["name"] == "meu-servidor-fs"  
    assert any("sanitizado" in w for w in warnings)  
    from gateway.config import GatewayConfig  
  
    GatewayConfig.model_validate(config)  
  
  
def test_prefixo_e_aplicado(tmp_path: Path) -> None:  
    source = write_source(tmp_path, {"fs": dict(SIMPLE_STDIO_ENTRY)})  
    config, _ = importer.import_config(source, prefix="claude")  
    assert config["backends"][0]["name"] == "claude.fs"  
  
  
def test_nomes_duplicados_apos_sanitizacao_geram_aviso(tmp_path: Path) -> None:  
    """'a b' e 'a-b' colidem após sanitização — o segundo é sinalizado."""  
    source = write_source(  
        tmp_path,  
        {  
            "a b": dict(SIMPLE_STDIO_ENTRY),  
            "a-b": dict(SIMPLE_STDIO_ENTRY),  
        },  
    )  
    config, warnings = importer.import_config(source)  
    assert [b["name"] for b in config["backends"]] == ["a-b"]  
    assert any("já usado" in w for w in warnings)  
  
  
def test_campos_desconhecidos_geram_aviso(tmp_path: Path) -> None:  
    source = write_source(  
        tmp_path,  
        {"x": {**SIMPLE_STDIO_ENTRY, "cwd": "/tmp", "shell": True}},  
    )  
    config, warnings = importer.import_config(source)  
    assert len(config["backends"]) == 1  
    assert any("cwd, shell" in w for w in warnings)  
  
  
def _run_cli(  
    source: Path, capsys: pytest.CaptureFixture[str], *args: str  
) -> tuple[int, str, str]:  
    """Roda main(argv) capturando stdout/stderr (fixture capsys do pytest)."""  
    exit_code = importer.main([str(source), *args])  
    captured = capsys.readouterr()  
    return exit_code, captured.out, captured.err  
  
  
def test_cli_default_grava_em_config_imported(  
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]  
) -> None:  
    source = write_source(tmp_path, {"fs": dict(SIMPLE_STDIO_ENTRY)})  
    monkeypatch.chdir(tmp_path)  
    exit_code, out, _ = _run_cli(source, capsys)  
    assert exit_code == 0  
    assert "config.imported.json" in out  
    generated = tmp_path / "config" / "config.imported.json"  
    assert generated.exists()  
    payload = json.loads(generated.read_text(encoding="utf-8"))  
    assert payload["backends"][0]["name"] == "fs"  
  
  
def test_cli_nunca_sobrescreve_sem_force(  
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]  
) -> None:  
    """Primeira gravação cria o arquivo; segunda SEM --force falha com código 2."""  
    source = write_source(tmp_path, {"fs": dict(SIMPLE_STDIO_ENTRY)})  
    monkeypatch.chdir(tmp_path)  
    assert _run_cli(source, capsys)[0] == 0  
    exit_code, _, err = _run_cli(source, capsys)  
    assert exit_code == 2  
    assert "--force" in err  
    exit_code, _, _ = _run_cli(source, capsys, "--force")  # com --force, sobrescreve  
    assert exit_code == 0  
  
  
def test_cli_stdout_imprime_json_sem_gravar(  
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]  
) -> None:  
    """--stdout imprime JSON puro (avisos vão no stderr) e nada é gravado."""  
    source = write_source(  
        tmp_path,  
        {  
            "fs": dict(SIMPLE_STDIO_ENTRY),  
            "notion": {"command": "npx", "args": ["-y", "mcp-remote", "https://x"]},  
        },  
    )  
    monkeypatch.chdir(tmp_path)  
    exit_code, out, err = _run_cli(source, capsys, "--stdout")  
    assert exit_code == 0  
    payload = json.loads(out)  
    assert [b["name"] for b in payload["backends"]] == ["fs"]  
    assert "mcp-remote" in err  # o não-convertível é sinalizado  
    assert not (tmp_path / "config" / "config.imported.json").exists()  
  
  
def test_cli_output_customizado(  
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]  
) -> None:  
    source = write_source(tmp_path, {"fs": dict(SIMPLE_STDIO_ENTRY)})  
    monkeypatch.chdir(tmp_path)  
    destino = tmp_path / "outro" / "meu-config.json"  
    exit_code, _, _ = _run_cli(source, capsys, "--output", str(destino))  
    assert exit_code == 0  
    assert destino.exists()  
  
  
def test_cli_list_servers(  
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]  
) -> None:  
    source = write_source(  
        tmp_path,  
        {  
            "fs": dict(SIMPLE_STDIO_ENTRY),  
            "remoto": {"type": "http", "url": "http://x"},  
        },  
    )  
    exit_code, out, _ = _run_cli(source, capsys, "--list-servers")  
    assert exit_code == 0  
    assert "fs" in out and "remoto" in out  
    assert "npx" in out and "http://x" in out  
  
  
def test_cli_list_servers_json_malformado(  
    tmp_path: Path, capsys: pytest.CaptureFixture[str]  
) -> None:  
    source = tmp_path / "claude_desktop_config.json"  
    source.write_text("{ not json", encoding="utf-8")  
  
    exit_code, out, err = _run_cli(source, capsys, "--list-servers")  
  
    assert exit_code == 2  
    assert out == ""  
    assert "ERRO:" in err  
    assert "JSONDecodeError" not in err  
  
  
def test_cli_list_servers_json_valido_preserva_saida(  
    tmp_path: Path, capsys: pytest.CaptureFixture[str]  
) -> None:  
    source = write_source(tmp_path, {"fs": dict(SIMPLE_STDIO_ENTRY)})  
  
    exit_code, out, err = _run_cli(source, capsys, "--list-servers")  
  
    assert exit_code == 0  
    assert err == ""  
    assert out == f"Servidores em {source} (1):\n  - fs: npx\n"  
  
  
def test_cli_sem_nada_convertivel_retorna_erro(  
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]  
) -> None:  
    source = write_source(  
        tmp_path,  
        {"notion": {"command": "npx", "args": ["-y", "mcp-remote", "https://x"]}},  
    )  
    monkeypatch.chdir(tmp_path)  
    exit_code, out, err = _run_cli(source, capsys)  
    assert exit_code == 1  
    assert "Nada a gravar" in err  
    assert not (tmp_path / "config" / "config.imported.json").exists()