"""Configuração do Gateway: modelos Pydantic + loader do config.json."""

import json
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, model_validator

DEFAULT_MAX_PAYLOAD_BYTES = 10 * 1024 * 1024  # 10 MiB
DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_RESTART_ATTEMPTS = 5
DEFAULT_BACKEND_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_SESSION_TTL_SECONDS = 3600.0  # sessão do filtro seletivo sem atividade
BACKEND_NAME_PATTERN = r"^[A-Za-z0-9_-]+$"


class BackendType(str, Enum):
    """Transporte do backend MCP (Fase 3 do ROADMAP)."""

    STDIO = "stdio"  # processo filho: JSON-RPC newline-delimited via stdin/stdout
    HTTP = "http"  # servidor remoto: JSON-RPC por POST na url
    SSE = "sse"  # servidor remoto: POST /messages + stream GET de Server-Sent Events


class BackendConfig(BaseModel):
    """Descrição de um backend MCP no config.json.

    O conjunto de campos obrigatórios depende do ``type``:
    - ``stdio``: ``command`` (e ``args`` opcional);
    - ``http``/``sse``: ``url`` (e ``headers`` opcional), sem command/args.
    A validação cruzada vive no ``model_validator`` no fim da classe.
    """

    name: str = Field(
        min_length=1,
        pattern=BACKEND_NAME_PATTERN,
        description=(
            "Nome único do backend (vira prefixo de namespace). Sem pontos nem"
            " espaços: o namespace é delimitado por '.', então pontos no nome"
            " tornariam o prefixo ambíguo."
        ),
    )
    type: BackendType = Field(
        default=BackendType.STDIO,
        description=(
            "Transporte do backend. Default 'stdio' — configs da Fase 0/1 sem o"
            " campo continuam válidos."
        ),
    )
    command: str | None = Field(default=None, description="Comando que sobe o processo MCP (só stdio).")
    args: list[str] = Field(default_factory=list, description="Argumentos do comando (só stdio).")
    url: str | None = Field(
        default=None,
        description="URL base do backend remoto (obrigatório para http/sse).",
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Headers HTTP extras enviados a backends http/sse (ex.: auth do próprio backend).",
    )
    request_timeout_seconds: float | None = Field(
        default=None,
        gt=0,
        description=(
            "Timeout (s) de cada request a este backend. Default:"
            f" {DEFAULT_BACKEND_REQUEST_TIMEOUT_SECONDS} quando ausente."
        ),
    )

    @property
    def effective_request_timeout(self) -> float:
        """Timeout de request com o default aplicado."""
        if self.request_timeout_seconds is not None:
            return self.request_timeout_seconds
        return DEFAULT_BACKEND_REQUEST_TIMEOUT_SECONDS

    @model_validator(mode="after")
    def _validate(self) -> "BackendConfig":
        if self.type is BackendType.STDIO:
            if not self.command:
                raise ValueError(
                    f"backend '{self.name}': 'command' é obrigatório quando type='stdio'"
                )
            if self.url:
                raise ValueError(
                    f"backend '{self.name}': 'url' não se aplica a backends stdio"
                )
        else:
            if not self.url:
                raise ValueError(
                    f"backend '{self.name}': 'url' é obrigatório quando type='{self.type.value}'"
                )
            if self.command is not None:
                raise ValueError(
                    f"backend '{self.name}': 'command'/'args' só se aplicam a backends stdio"
                    " (use 'url')"
                )
        return self


class GatewayConfig(BaseModel):
    """Configuração raiz do Gateway."""

    backends: list[BackendConfig]
    auth_token: str | None = Field(
        default=None,
        min_length=1,
        description=(
            "Token Bearer opcional para a rota POST /mcp. Se ausente, o Gateway"
            " roda sem autenticação (uso local) e avisa no log de startup."
        ),
    )
    max_payload_bytes: int = Field(
        default=DEFAULT_MAX_PAYLOAD_BYTES,
        gt=0,
        description="Limite de tamanho do corpo do POST /mcp (HTTP 413 acima disso).",
    )
    health_check_interval_seconds: float = Field(
        default=DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS,
        gt=0,
        description=(
            "Intervalo entre ciclos do Health Monitor, que verifica se cada backend"
            " continua saudável (Fase 2)."
        ),
    )
    backend_request_timeout_seconds: float | None = Field(
        default=None,
        gt=0,
        description=(
            "Timeout global (s) de cada request a um backend. Pode ser"
            " sobreescrito por backend em request_timeout_seconds."
        ),
    )
    auto_restart: bool = Field(
        default=True,
        description=(
            "Se o Gateway tenta reiniciar automaticamente backends detectados como"
            " offline pelo Health Monitor (Fase 2)."
        ),
    )
    max_restart_attempts: int = Field(
        default=DEFAULT_MAX_RESTART_ATTEMPTS,
        ge=1,
        description=(
            "Máximo de tentativas de restart consecutivas de um backend antes de"
            " marcá-lo como 'failed' (estado terminal: exige intervenção humana"
            " reiniciando o Gateway)."
        ),
    )
    session_ttl_seconds: float = Field(
        default=DEFAULT_SESSION_TTL_SECONDS,
        gt=0,
        description=(
            "Tempo de vida (s) de uma sessão do filtro seletivo de backends sem"
            " atividade (Fase 5). A cada request com o mesmo Mcp-Session-Id o TTL"
            " é renovado; expirada, a sessão volta a ver todos os backends."
        ),
    )

    def request_timeout_for(self, backend: BackendConfig) -> float:
        """Timeout de request efetivo de um backend: o específico vence o global."""
        if backend.request_timeout_seconds is not None:
            return backend.request_timeout_seconds
        if self.backend_request_timeout_seconds is not None:
            return self.backend_request_timeout_seconds
        return DEFAULT_BACKEND_REQUEST_TIMEOUT_SECONDS

    @model_validator(mode="after")
    def _validate(self) -> "GatewayConfig":
        if self.auth_token == "":
            raise ValueError("auth_token não pode ser uma string vazia (omitir ou usar um token)")
        if not self.backends:
            raise ValueError("config.json deve definir ao menos um backend")
        names = [backend.name for backend in self.backends]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"nomes de backends duplicados no config.json: {duplicates}")
        return self


def load_config(path: Path) -> GatewayConfig:
    """Carrega e valida o config.json, levantando ValueError com erro claro."""
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"arquivo de configuração não encontrado: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"config.json inválido (JSON malformado): {exc}") from exc
    return GatewayConfig.model_validate(raw)