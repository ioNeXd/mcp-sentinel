#!/usr/bin/env python
"""Importador de ``claude_desktop_config.json`` para o config do McpSentinel (Fase 4).

Lê a seção ``mcpServers`` do config do Claude Desktop e converte cada entrada
para o formato de backends do Gateway:

- entradas com ``command``/``args`` viram backends ``type: "stdio"``;
- entradas com ``url`` + ``type`` (http/sse) são convertidas diretamente — o
  formato remoto do Claude Desktop já coincide com o do Gateway;
- entradas que usam uma PONTE para servidor remoto (``mcp-remote``,
  ``supergateway``, ``mcp-proxy``...) NÃO são convertidas automaticamente —
  são sinalizadas com um aviso claro. A ponte vira um processo stdio do ponto
  de vista do Gateway (funcionaria!), mas esconderia a conexão remota dentro
  de um subprocesso: perde-se o ``type: "http"/"sse"`` nativo (health check de
  verdade, reconexão sem processo filho) e a decisão de qual URL/transporte
  usar é do operador, não do importador.

Segurança em relação ao ``config/config.json`` existente: o importador NUNCA
sobrescreve silenciosamente. Sem flags, grava em ``config/config.imported.json``
(para revisão e mesclagem manual — o Gateway não tem hot-reload, então mesclar
é uma decisão do operador); ``--output CAMINHO`` escolhe outro destino; e
qualquer destino que já exista exige ``--force`` explícito.

Uso::

    python scripts/import_claude_desktop_config.py [CAMINHO] [opções]

    CAMINHO          config do Claude Desktop (default: busca automatica nos
                     caminhos conhecidos -- instalacao classica ou MSIX no
                     Windows, macOS, Linux -- ver find_claude_desktop_config)
    --output, -o     arquivo de destino (default: config/config.imported.json)
    --force          sobrescreve o destino se ele já existir
    --stdout         só imprime o JSON resultante (não grava nada)
    --prefix NOME    prefixa "NOME-" em todos os backends importados (útil
                     para evitar colisão de nomes ao mesclar com um config
                     que já tem backends)
    --list-servers   só lista o que foi encontrado, sem converter
    -v               imprime os avisos também no stderr

Limitações conhecidas (documentadas no README):
- ``env`` por servidor do Claude não é aplicado (o Gateway não suporta env
  por backend) — a entrada é convertida e o campo sinalizado em aviso;
- ``cwd``, ``shell`` e campos desconhecidos são ignorados com aviso;
- pontes (mcp-remote e similares) não são convertidas — ver acima;
- ``url`` sem ``type`` não é convertida (o Gateway exige declarar o transporte);
- variáveis ``%ENV%``/``$ENV`` em command/args são expandidas best-effort com
  os valores atuais do ambiente (o Gateway não expande variáveis).
"""

import argparse
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

#: Padrão de nome válido para backend do Gateway (ver ``gateway/config.py``):
#: nomes viram prefixo de namespace delimitado por ``.``, então só
#: letras/números/underscore/hífen são aceitos.
VALID_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")

#: Pontes conhecidas que escondem um servidor remoto atrás de um processo
#: stdio. Qualquer script cujo nome contenha um destes termos também é
#: sinalizado — detecção de melhor esforço, sem pretensão de ser completa.
KNOWN_BRIDGES = {"mcp-remote", "supergateway", "mcp-proxy"}
BRIDGE_NAME_HINTS = ("remote", "proxy", "gateway", "bridge")

#: Destino default quando ``--output`` não é informado (arquivo de revisão,
#: nunca o ``config/config.json`` do Gateway em si).
DEFAULT_OUTPUT = Path("config/config.imported.json")


def _load_config_json(source_path: Path) -> Any:
    """Lê e faz parse do JSON, convertendo erros em mensagens claras."""
    try:
        return json.loads(source_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"arquivo não encontrado: {source_path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON malformado em {source_path}: {exc}") from exc


def sanitize_backend_name(raw_name: str) -> str:
    """Converte um nome de servidor do Claude para o padrão do Gateway.

    Substitui caracteres inválidos (ex.: espaços, pontos) por ``-`` e devolve
    o nome pronto para ser prefixo de namespace. Pode ficar vazio se a entrada
    só tiver caracteres inválidos — o chamador decide o que fazer.
    """
    cleaned = re.sub(r"[^A-Za-z0-9_-]", "-", raw_name.strip())
    return cleaned.strip("-")


def _looks_like_bridge(tokens: list[str]) -> bool:
    """Detecta best-effort se a linha de comando é uma ponte para servidor remoto.

    Varre o comando E os argumentos: com ``npx``/``uvx`` a ponte aparece nos
    args (ex.: ``["-y", "mcp-remote", "https://..."]``), não no comando.
    Regras por token (basename, sem extensão):

    - igual a uma ponte conhecida (mcp-remote, supergateway, mcp-proxy) → sim;
    - contém um dos hints (remote/proxy/gateway/bridge) → sim — exceto tokens
      que são flags (começam com "-"), URLs (contêm "://") ou caminhos com
      extensão de documento, para limitar falsos positivos.
    """
    for token in tokens:
        if not token:
            continue
        parts = token.replace("\\", "/").split("/")
        base = parts[-1].lower()
        stem = Path(base).stem.lower()
        if stem in KNOWN_BRIDGES or base in KNOWN_BRIDGES:
            return True
        if token.startswith("-") or "://" in token:
            continue
        if "." in base and base.rsplit(".", 1)[1] in {"json", "md", "txt", "yaml", "yml", "toml"}:
            continue
        if any(hint in stem for hint in BRIDGE_NAME_HINTS):
            return True
    return False


def _expand_env(value: str) -> str:
    """Expande variáveis de ambiente best-effort (%VAR% no Windows, $VAR no POSIX)."""
    return os.path.expandvars(value)


def convert_entry(name: str, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    """Converte uma entrada de ``mcpServers`` para o formato do Gateway.

    Devolve ``(backend_ou_None, avisos)``. ``None`` + aviso significa entrada
    NÃO convertida (nunca é descartada silenciosamente). A ordem de decisão é:
    ``env`` é sempre sinalizado (o Gateway não o aplica); em seguida tenta o
    caminho remoto (``url`` + ``type``, que exige ``command`` AUSENTE), depois
    a detecção de ponte, e por último o caminho stdio comum. Em AMBOS os
    caminhos de sucesso (remoto e stdio), campos não consumidos geram aviso
    de "campos ignorados" — nunca descarte silencioso. Entrada com ``url`` E
    ``command`` vira stdio com aviso explícito de ambiguidade (a ``url`` é
    ignorada de propósito, não é esquecida).
    """
    warnings: list[str] = []
    if not isinstance(entry, dict):
        return None, [f"[{name}] entrada não é um objeto: {entry!r} — ignorada"]

    if entry.get("env"):
        warnings.append(
            f"[{name}] campo 'env' não é aplicado — o Gateway não suporta "
            "variáveis de ambiente por backend (a ferramenta pode não funcionar "
            "se depender delas)"
        )

    url = entry.get("url")
    if url and not entry.get("command"):
        remote_type = entry.get("type")
        if remote_type in ("http", "sse"):
            backend: dict[str, Any] = {"name": name, "type": remote_type, "url": url}
            if entry.get("headers"):
                backend["headers"] = dict(entry["headers"])
            # Mesma simetria do ramo stdio: campos não consumidos nunca são
            # descartados em silêncio. "env" já foi sinalizado no topo e
            # "headers" é consumido acima (mesmo vazio/falsy).
            unknown = sorted(set(entry) - {"url", "type", "headers", "env"})
            if unknown:
                warnings.append(f"[{name}] campos ignorados: {', '.join(unknown)}")
            return backend, warnings
        return None, [
            f"[{name}] entrada com 'url' mas sem 'type': 'http'/'sse' — o "
            "Gateway exige o transporte declarado; não convertido (edite à mão "
            'adicionando "type": "http" ou "sse")'
        ]

    command = entry.get("command")
    if not isinstance(command, str) or not command:
        return None, [f"[{name}] sem 'command' nem 'url' utilizável — ignorada"]

    args = entry.get("args") or []
    if not isinstance(args, list) or not all(isinstance(a, str) for a in args):
        return None, [f"[{name}] 'args' não é uma lista de strings — ignorada"]

    if _looks_like_bridge([command, *args]):
        return None, [
            f"[{name}] usa uma ponte para servidor remoto ('{command}' com "
            f"args {args}) — não conversível automaticamente. Declare o backend "
            'diretamente no formato do Gateway: {"type": "http", "url": "..."} '
            'ou {"type": "sse", "url": "..."} (a URL real está nos args da ponte)'
        ]

    if " " in command:
        warnings.append(
            f"[{name}] 'command' contém espaços ('{command}') — no Gateway o "
            "comando é um executável único e argumentos vão em 'args'; se isto "
            "era uma linha de shell, separe-o manualmente"
        )

    # Ambiguidade url+command (o ramo remoto exige command AUSENTE): com ambos
    # presentes, a escolha é stdio — mas isso precisa ser EXPLÍCITO, porque a
    # entrada podia ser um servidor remoto mal declarado. O aviso substitui a
    # listagem genérica de "url" em 'campos ignorados' (ela sai do conjunto
    # known abaixo), então não há menção dupla.
    if url:
        warnings.append(
            f"[{name}] entrada tem 'url' E 'command' — tratada como stdio "
            "(command/args) e a 'url' é IGNORADA; se for um servidor remoto, "
            'declare {"type": "http"|"sse", "url": "..."} sem command/args'
        )

    backend = {
        "name": name,
        "type": "stdio",
        "command": _expand_env(command),
        "args": [_expand_env(arg) for arg in args],
    }
    ignored = set(entry) - {"command", "args", "env", "type"}
    if url:  # url vazia/ausente não teve aviso: segue na lista genérica
        ignored.discard("url")
    unknown = sorted(ignored)
    if unknown:
        warnings.append(f"[{name}] campos ignorados: {', '.join(unknown)}")
    return backend, warnings


def import_config(source_path: Path, prefix: str = "") -> tuple[dict[str, Any], list[str]]:
    """Lê o config do Claude e devolve ``(gateway_config, avisos)``.

    Para cada servidor: sanitiza o nome, resolve colisões (a primeira entrada
    convertida vence; as seguintes com o mesmo nome são sinalizadas e puladas)
    e aplica o ``prefix`` opcional antes de delegar a conversão a
    ``convert_entry``.

    O prefixo é sanitizado como qualquer nome e composto com ``-`` — NÃO com
    ``.``: o nome de backend é validado contra o padrão do Gateway
    (``BACKEND_NAME_PATTERN``, que proíbe ``.`` por ser o delimitador do
    namespace ``backend.<id>``), então um prefixo composto com ``.``
    (``claude.meu-server``) produziria um config.json que o Gateway rejeita
    no boot. Prefixo sem nenhum caractere válido não aborta a importação:
    é sinalizado e ignorado.

    Levanta ``ValueError`` com mensagem clara para arquivos ausentes,
    malformados ou sem a seção ``mcpServers``.
    """
    raw = _load_config_json(source_path)
    if not isinstance(raw, dict) or not isinstance(raw.get("mcpServers"), dict):
        raise ValueError(
            f"{source_path} não tem a seção 'mcpServers' (isso é um claude_desktop_config.json?)"
        )

    backends: list[dict[str, Any]] = []
    warnings: list[str] = []
    used_names: set[str] = set()
    clean_prefix = sanitize_backend_name(prefix)
    if prefix and not clean_prefix:
        warnings.append(
            f"prefixo '{prefix}' sem nenhum caractere válido (letras, números, "
            "'_' ou '-') — importando sem prefixo"
        )
    elif prefix and clean_prefix != prefix:
        warnings.append(
            f"prefixo '{prefix}' sanitizado para '{clean_prefix}' (o Gateway "
            "não aceita espaços/pontos — viram prefixo de namespace)"
        )
    for server_name, entry in raw["mcpServers"].items():
        clean = sanitize_backend_name(str(server_name))
        if not clean:
            warnings.append(
                f"[{server_name}] nome não tem nenhum caractere válido "
                "(letras, números, '_' ou '-') — entrada ignorada"
            )
            continue
        if clean != server_name:
            warnings.append(
                f"[{server_name}] nome sanitizado para '{clean}' (o Gateway "
                "não aceita espaços/pontos — viram prefixo de namespace)"
            )
        if clean_prefix:
            # Separador '-' e não '.': o nome de backend não pode conter '.'
            # (é o delimitador do namespace; BACKEND_NAME_PATTERN rejeita), e
            # um prefixo composto com '.' geraria backends rejeitados no boot.
            clean = f"{clean_prefix}-{clean}"
        if clean in used_names:
            warnings.append(
                f"[{server_name}] nome '{clean}' já usado por outra entrada "
                "convertida — ajuste à mão no arquivo gerado"
            )
            continue
        used_names.add(clean)
        backend, entry_warnings = convert_entry(clean, entry)
        warnings.extend(entry_warnings)
        if backend is not None:
            backends.append(backend)
        else:
            # entrada falhou a conversão — libera o nome pra uma eventual
            # entrada posterior com o mesmo nome sanitizado.
            used_names.discard(clean)

    gateway_config: dict[str, Any] = {"backends": backends}
    if not backends:
        warnings.append(
            "nenhum servidor foi convertido — o config gerado ficaria vazio "
            "(o Gateway exige ao menos um backend)"
        )
    return gateway_config, warnings


def _candidate_claude_config_paths() -> list[Path]:
    """Caminhos conhecidos do ``claude_desktop_config.json``, em ordem de checagem.

    O Claude Desktop no Windows pode estar instalado de duas formas bem
    diferentes, que resultam em caminhos completamente distintos:

    - **instalador clássico**: ``%APPDATA%\\Claude\\claude_desktop_config.json``;
    - **pacote MSIX** (Microsoft Store / algumas distribuições): o app roda em
      sandbox e o config real fica em
      ``%LOCALAPPDATA%\\Packages\\Claude_<hash>\\LocalCache\\Roaming\\Claude\\
      claude_desktop_config.json`` — o ``<hash>`` varia por instalação, então a
      busca usa um glob (``Claude_*``) em vez de um caminho fixo.

    Cobre também macOS e Linux (o Gateway hoje só é testado no Windows, mas a
    checagem é barata e não custa nada incluir).
    """
    candidates: list[Path] = []
    appdata = os.environ.get("APPDATA")
    if appdata:
        candidates.append(Path(appdata) / "Claude" / "claude_desktop_config.json")
    localappdata = os.environ.get("LOCALAPPDATA")
    if localappdata:
        packages_dir = Path(localappdata) / "Packages"
        if packages_dir.is_dir():
            for entry in sorted(packages_dir.glob("Claude_*")):
                candidates.append(
                    entry / "LocalCache" / "Roaming" / "Claude" / "claude_desktop_config.json"
                )
    home = Path.home()
    candidates.append(
        home / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    )
    candidates.append(home / ".config" / "Claude" / "claude_desktop_config.json")
    return candidates


def find_claude_desktop_config() -> Path | None:
    """Primeiro caminho candidato (ver ``_candidate_claude_config_paths``) que
    existe de fato no disco, ou ``None`` se nenhum bater — quem chama decide
    o que fazer nesse caso (CLI pede pra informar manualmente; o dashboard
    abre um campo pra apontar o arquivo)."""
    for candidate in _candidate_claude_config_paths():
        if candidate.exists():
            return candidate
    return None


def _write_output(destination: Path, content: str, force: bool) -> bool:
    """Grava o arquivo respeitando a regra de nunca sobrescrever sem --force.

    Devolve ``False`` (sem gravar nada) quando o destino já existe e --force
    não foi passado — a mensagem de erro já foi impressa no stderr.

    A escrita é ATÔMICA: o conteúdo vai primeiro para um arquivo temporário no
    MESMO diretório do destino e só então é promovido com ``os.replace`` (rename
    atômico dentro do mesmo filesystem). Assim uma falha no meio da gravação
    (disco cheio, processo morto) nunca deixa o destino truncado/corrompido —
    ou ele fica com o conteúdo antigo intacto, ou com o novo por completo. O
    temporário é removido em caso de erro para não deixar lixo.

    Segurança: path traversal bloqueado — destino deve estar dentro do diretório
    do projeto (cwd ou subdiretórios). Paths absolutos ou com '..' que apontam
    fora são rejeitados.
    """
    # Bloqueia path traversal: resolve o destino e verifica se está dentro do cwd.
    # ponytail: sandboxing mais rigoroso (chroot/AppArmor) quando necessário.
    try:
        resolved = destination.resolve()
        cwd = Path.cwd().resolve()
        # Checa se resolved é subpath de cwd (ou o próprio cwd).
        resolved.relative_to(cwd)
    except (ValueError, OSError) as exc:
        print(
            f"ERRO: path '{destination}' inválido ou fora do diretório do projeto. "
            f"Path traversal não permitido. ({exc})",
            file=sys.stderr,
        )
        return False
    if destination.exists() and not force:
        print(
            f"ERRO: {destination} já existe. Use --force para sobrescrever "
            "(ou revise/mescle manualmente).",
            file=sys.stderr,
        )
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Nome UNICO por execucao (mkstemp no mesmo diretorio):
    # o nome fixo corrompia escritas concorrentes.
    fd, tmp_path = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp_path, destination)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return True


def _build_arg_parser() -> argparse.ArgumentParser:
    """Monta o parser da CLI (mantido separado de ``main`` para clareza)."""
    parser = argparse.ArgumentParser(
        description=(
            "Importa mcpServers do claude_desktop_config.json para o formato "
            "do MCP Gateway. Nunca sobrescreve um arquivo existente sem --force."
        )
    )
    parser.add_argument(
        "config",
        nargs="?",
        default=None,
        help="caminho do claude_desktop_config.json (default: caminho do Windows)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"arquivo de destino (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="sobrescreve o arquivo de destino se ele já existir",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="imprime o JSON resultante em vez de gravar arquivo",
    )
    parser.add_argument(
        "--prefix",
        default="",
        help='prefixa "NOME-" nos backends importados (ex.: --prefix claude)',
    )
    parser.add_argument(
        "--list-servers",
        action="store_true",
        help="só lista os servidores encontrados, sem converter",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="avisos também no stderr")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entrada da CLI. Devolve o exit code do processo.

    Códigos: ``0`` sucesso (ou --stdout/--list-servers), ``1`` quando nenhum
    backend foi convertido (nada a gravar) e ``2`` para erros de entrada
    (arquivo ausente/malformado, destino já existente sem --force).
    """
    parser = _build_arg_parser()
    options = parser.parse_args(argv)

    source = Path(options.config) if options.config else find_claude_desktop_config()
    if source is None:
        parser.error(
            "não encontrei o claude_desktop_config.json em nenhum dos caminhos "
            "conhecidos (instalação clássica ou MSIX) — informe o caminho "
            "manualmente como argumento"
        )
    if not source.exists():
        print(f"ERRO: arquivo não encontrado: {source}", file=sys.stderr)
        return 2

    if options.list_servers:
        try:
            raw = _load_config_json(source)
        except ValueError as exc:
            print(f"ERRO: {exc}", file=sys.stderr)
            return 2
        servers = raw.get("mcpServers") if isinstance(raw, dict) else None
        if not isinstance(servers, dict):
            print("ERRO: seção 'mcpServers' não encontrada.", file=sys.stderr)
            return 2
        print(f"Servidores em {source} ({len(servers)}):")
        for name, entry in servers.items():
            command = entry.get("command") if isinstance(entry, dict) else None
            url = entry.get("url") if isinstance(entry, dict) else None
            origin = command or url or "?"
            print(f"  - {name}: {origin}")
        return 0

    try:
        gateway_config, warnings = import_config(source, prefix=options.prefix)
    except ValueError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 2

    pretty = json.dumps(gateway_config, indent=2, ensure_ascii=False)

    # No modo --stdout o JSON é o ÚNICO conteúdo do stdout (consumível por
    # pipe/redirecionamento); resumo e avisos vão todos para o stderr.
    if options.stdout:
        print(pretty)
    elif options.verbose:
        print(pretty)

    for warning in warnings:
        print(f"AVISO: {warning}", file=sys.stderr)

    converted = len(gateway_config["backends"])
    summary = f"Convertidos: {converted} backend(s) | Avisos: {len(warnings)} | Origem: {source}"
    if converted == 0:
        print(summary)
        print(
            "Nada a gravar (nenhum backend convertido). Revise os avisos acima.",
            file=sys.stderr,
        )
        return 1

    if options.stdout:
        print(summary, file=sys.stderr)
        print(
            "Modo --stdout: nada foi gravado. Revise e grave com --output/-o.",
            file=sys.stderr,
        )
        return 0
    print(summary)

    if not _write_output(options.output, pretty + "\n", options.force):
        return 2
    print(f"Config gerado em: {options.output}")
    print(
        "Próximos passos: revise o arquivo, mescle os backends que quiser no "
        "config/config.json do Gateway (ou aponte MCP_GATEWAY_CONFIG para ele) "
        "e reinicie o Gateway (não há hot-reload)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
