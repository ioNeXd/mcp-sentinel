"""Configuração do Gateway: modelos Pydantic + loader do config.json."""  
  
import json  
import re  
import warnings  
from collections import Counter  
from enum import Enum  
from pathlib import Path  
from typing import Any  
from urllib.parse import urlparse  
  
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator  
  
DEFAULT_MAX_PAYLOAD_BYTES = 10 * 1024 * 1024  # 10 MiB  
DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS = 5.0  
DEFAULT_MAX_RESTART_ATTEMPTS = 5  
DEFAULT_BACKEND_REQUEST_TIMEOUT_SECONDS = 30.0  
DEFAULT_SESSION_TTL_SECONDS = 3600.0  # sessão do filtro seletivo sem atividade  
BACKEND_NAME_PATTERN = r"^[A-Za-z0-9_-]+$"  
  
# Tetos superiores dos valores numéricos de configuração. Sem eles, um valor  
# absurdamente grande é aceito silenciosamente e vira um problema em runtime:  
# um timeout "infinito" prende um request para sempre; um intervalo/TTL gigante  
# equivale a desligar o mecanismo sem avisar; um payload de gigabytes convida a  
# exaustão de memória. Os limites abaixo são folgados (nenhum uso legítimo os  
# alcança) e servem só para transformar valores obviamente errados numa falha  
# de configuração clara, no load, em vez de um comportamento estranho depois.  
MAX_MAX_PAYLOAD_BYTES = 1024 * 1024 * 1024  # 1 GiB  
MAX_HEALTH_CHECK_INTERVAL_SECONDS = 86_400.0  # 1 dia  
MAX_BACKEND_REQUEST_TIMEOUT_SECONDS = 86_400.0  # 1 dia  
MAX_RESTART_ATTEMPTS = 1_000  
MAX_SESSION_TTL_SECONDS = 30 * 86_400.0  # 30 dias  
  
  
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
  
    @field_validator("url")  
    @classmethod  
    def _validate_url(cls, v: str | None) -> str | None:  
        """Valida que a URL usa esquema http/https e tem host não-vazio."""  
        if v is None:  
            return v  
        try:  
            parsed = urlparse(v)  
        except (ValueError, AttributeError) as exc:  
            raise ValueError(f"'url' não é uma URL válida: {exc}") from exc  
        if parsed.scheme not in ("http", "https"):  
            raise ValueError(  
                f"'url' precisa começar com http:// ou https:// (recebeu scheme='{parsed.scheme}')"  
            )  
        if not parsed.netloc:  
            raise ValueError("'url' precisa ter um host não-vazio (ex.: http://127.0.0.1:9000)")  
        return v  
  
    @field_validator("headers")  
    @classmethod  
    def _validate_headers(cls, v: dict[str, str]) -> dict[str, str]:  
        """Valida nomes e valores de headers conforme RFC 7230.  
  
        Nomes devem ser tokens HTTP válidos; valores não podem conter  
        quebras de linha (\\r ou \\n) — prevenindo header injection.  
        """  
        # RFC 7230 token pattern: caracteres permitidos em header field-name  
        token_pattern = re.compile(r"^[!#$%&'*+\-.^_|~0-9A-Za-z]+$")  
        for name, value in v.items():  
            if not token_pattern.match(name):  
                raise ValueError(  
                    f"header '{name}': nome inválido (não é um token HTTP válido conforme RFC 7230)"  
                )  
            if "\r" in value or "\n" in value:  
                raise ValueError(  
                    f"header '{name}': valor não pode conter quebras de linha (\\r ou \\n)"  
                )  
        return v  
  
    request_timeout_seconds: float | None = Field(  
        default=None,  
        gt=0,  
        le=MAX_BACKEND_REQUEST_TIMEOUT_SECONDS,  
        description=(  
            "Timeout (s) de cada request a este backend. Default:"  
            f" {DEFAULT_BACKEND_REQUEST_TIMEOUT_SECONDS} quando ausente."  
        ),  
    )  
  
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
            if self.command is not None or self.args:  
                raise ValueError(  
                    f"backend '{self.name}': 'command'/'args' só se aplicam a backends stdio"  
                    " (use 'url')"  
                )  
            # 2.3 — aviso se args foi declarado explicitamente (provavelmente resquício  
            # de copiar/colar de um config stdio): não é erro, mas é suspeito.  
            if "args" in self.model_fields_set and self.args == []:  
                warnings.warn(  
                    f"backend '{self.name}': 'args' declarado explicitamente como lista vazia "  
                    f"num backend {self.type.value} (args só se aplica a stdio — provável "  
                    "resquício de config stdio copiado/colado)",  
                    stacklevel=2,  
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
        le=MAX_MAX_PAYLOAD_BYTES,  
        description="Limite de tamanho do corpo do POST /mcp (HTTP 413 acima disso).",  
    )  
    health_check_interval_seconds: float = Field(  
        default=DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS,  
        gt=0,  
        le=MAX_HEALTH_CHECK_INTERVAL_SECONDS,  
        description=(  
            "Intervalo entre ciclos do Health Monitor, que verifica se cada backend"  
            " continua saudável (Fase 2)."  
        ),  
    )  
    backend_request_timeout_seconds: float | None = Field(  
        default=None,  
        gt=0,  
        le=MAX_BACKEND_REQUEST_TIMEOUT_SECONDS,  
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
        le=MAX_RESTART_ATTEMPTS,  
        description=(  
            "Máximo de tentativas de restart consecutivas de um backend antes de"  
            " marcá-lo como 'failed' (estado terminal: exige intervenção humana"  
            " reiniciando o Gateway)."  
        ),  
    )  
    session_ttl_seconds: float = Field(  
        default=DEFAULT_SESSION_TTL_SECONDS,  
        gt=0,  
        le=MAX_SESSION_TTL_SECONDS,  
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
        if not self.backends:  
            raise ValueError("config.json deve definir ao menos um backend")  
        name_counts = Counter(backend.name for backend in self.backends)  
        duplicates = sorted(name for name, count in name_counts.items() if count > 1)  
        if duplicates:  
            raise ValueError(f"nomes de backends duplicados no config.json: {duplicates}")  
        return self  
  
  
def load_config(path: Path) -> GatewayConfig:  
    """Carrega e valida o config.json, levantando ValueError com erro claro.  
  
    Captura toda falha de leitura/parse/validação e converta em ValueError,  
    para que o main.py (except ValueError) receba sempre mensagem legível.  
    """  
    try:  
        raw: Any = json.loads(path.read_text(encoding="utf-8"))  
    except FileNotFoundError as exc:  
        raise ValueError(f"arquivo de configuração não encontrado: {path}") from exc  
    except json.JSONDecodeError as exc:  
        raise ValueError(f"config.json inválido em {path} (JSON malformado): {exc}") from exc  
    except UnicodeDecodeError as exc:  
        raise ValueError(  
            f"config.json em {path} não é UTF-8 válido: {exc}"  
        ) from exc  
    except OSError as exc:  
        # Cobre erros de I/O não cobertos por FileNotFoundError (permissão  
        # negada, é um diretório, etc.).  
        raise ValueError(f"erro ao ler configuração em {path}: {exc}") from exc  
    try:  
        return GatewayConfig.model_validate(raw)  
    except ValidationError as exc:  
        raise ValueError(  
            f"config.json inválido em {path}: {exc}"  
        ) from exc