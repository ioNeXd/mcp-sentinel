"""Configuração do Gateway: modelos Pydantic e loader do ``config.json``.  
  
Define o esquema declarativo do arquivo de configuração — um ``BackendConfig``  
por servidor MCP agregado, agrupados sob um ``GatewayConfig`` raiz — e a função  
``load_config``, único ponto de entrada de leitura do disco.  
  
Toda validação (campos obrigatórios por transporte, unicidade de nomes, limites  
numéricos, sanidade de URL/headers) acontece na construção dos modelos, de modo  
que o restante do Gateway pode assumir uma configuração já íntegra.  
"""  
  
import json  
import re  
import warnings  
from collections import Counter  
from enum import Enum  
from pathlib import Path  
from typing import Any  
from urllib.parse import urlparse  
  
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator  
  
DEFAULT_MAX_PAYLOAD_BYTES = 10 * 1024 * 1024  
DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS = 5.0  
DEFAULT_MAX_RESTART_ATTEMPTS = 5  
DEFAULT_BACKEND_REQUEST_TIMEOUT_SECONDS = 30.0  
DEFAULT_SESSION_TTL_SECONDS = 3600.0  
BACKEND_NAME_PATTERN = r"^[A-Za-z0-9_-]+$"  
  
# Tetos superiores dos campos numéricos. Servem para transformar um valor  
# obviamente errado (timeout "infinito", intervalo/TTL que desliga o mecanismo  
# na prática, payload de gigabytes que convida à exaustão de memória) numa  
# falha de configuração clara no load, em vez de comportamento estranho em  
# runtime. São folgados de propósito: nenhum uso legítimo os alcança.  
MAX_MAX_PAYLOAD_BYTES = 1024 * 1024 * 1024  
MAX_HEALTH_CHECK_INTERVAL_SECONDS = 86_400.0  
MAX_BACKEND_REQUEST_TIMEOUT_SECONDS = 86_400.0  
MAX_RESTART_ATTEMPTS = 1_000  
MAX_SESSION_TTL_SECONDS = 30 * 86_400.0  
  
# Nomes permitidos em header field-name conforme RFC 7230 (token).  
_HEADER_NAME_TOKEN = re.compile(r"^[!#$%&'*+\-.^_|~0-9A-Za-z]+$")  
  
  
class BackendType(str, Enum):  
    """Transporte de comunicação com um backend MCP.  
  
    - ``STDIO``: processo filho falando JSON-RPC delimitado por newline via  
      stdin/stdout.  
    - ``HTTP``: servidor remoto que recebe JSON-RPC por POST na ``url``.  
    - ``SSE``: servidor remoto com POST em ``/messages`` mais um stream GET de  
      Server-Sent Events.  
    """  
  
    STDIO = "stdio"  
    HTTP = "http"  
    SSE = "sse"  
  
  
class BackendConfig(BaseModel):  
    """Descrição de um backend MCP no ``config.json``.  
  
    O conjunto de campos obrigatórios depende do ``type``: backends ``stdio``  
    exigem ``command`` (com ``args`` opcional) e rejeitam ``url``; backends  
    ``http``/``sse`` exigem ``url`` (com ``headers`` opcional) e rejeitam  
    ``command``/``args``. A validação cruzada vive em :meth:`_validate`.  
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
    command: str | None = Field(  
        default=None,  
        description="Comando que sobe o processo MCP (só stdio).",  
    )  
    args: list[str] = Field(  
        default_factory=list,  
        description="Argumentos do comando (só stdio).",  
    )  
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
        le=MAX_BACKEND_REQUEST_TIMEOUT_SECONDS,  
        description=(  
            "Timeout (s) de cada request a este backend. Default:"  
            f" {DEFAULT_BACKEND_REQUEST_TIMEOUT_SECONDS} quando ausente."  
        ),  
    )  
  
    @field_validator("url")  
    @classmethod  
    def _validate_url(cls, v: str | None) -> str | None:  
        """Exige esquema http/https e host não-vazio quando ``url`` é fornecida."""  
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
  
        Nomes precisam ser tokens HTTP válidos e valores não podem conter  
        quebras de linha (``\\r``/``\\n``), fechando a porta para header  
        injection via config.  
        """  
        for name, value in v.items():  
            if not _HEADER_NAME_TOKEN.match(name):  
                raise ValueError(  
                    f"header '{name}': nome inválido (não é um token HTTP válido conforme RFC 7230)"  
                )  
            if "\r" in value or "\n" in value:  
                raise ValueError(  
                    f"header '{name}': valor não pode conter quebras de linha (\\r ou \\n)"  
                )  
        return v  
  
    @model_validator(mode="after")  
    def _validate(self) -> "BackendConfig":  
        """Aplica a coerência entre ``type`` e os campos específicos do transporte.  
  
        Backends ``stdio`` exigem ``command`` e recusam ``url``; backends  
        remotos exigem ``url`` e recusam ``command``/``args``. Um ``args`` vazio  
        declarado explicitamente num backend remoto emite aviso (não erro), por  
        ser um resquício provável de um bloco stdio copiado/colado.  
        """  
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
            if "args" in self.model_fields_set and self.args == []:  
                warnings.warn(  
                    f"backend '{self.name}': 'args' declarado explicitamente como lista vazia "  
                    f"num backend {self.type.value} (args só se aplica a stdio — provável "  
                    "resquício de config stdio copiado/colado)",  
                    stacklevel=2,  
                )  
        return self  
  
  
class GatewayConfig(BaseModel):  
    """Configuração raiz do Gateway: a lista de backends e os parâmetros globais."""  
  
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
        """Resolve o timeout efetivo de um backend.  
  
        Precedência: o valor específico do backend vence o global, que por sua  
        vez vence o default do Gateway.  
        """  
        if backend.request_timeout_seconds is not None:  
            return backend.request_timeout_seconds  
        if self.backend_request_timeout_seconds is not None:  
            return self.backend_request_timeout_seconds  
        return DEFAULT_BACKEND_REQUEST_TIMEOUT_SECONDS  
  
    @model_validator(mode="after")  
    def _validate(self) -> "GatewayConfig":  
        """Exige ao menos um backend e nomes únicos entre eles."""  
        if not self.backends:  
            raise ValueError("config.json deve definir ao menos um backend")  
        name_counts = Counter(backend.name for backend in self.backends)  
        duplicates = sorted(name for name, count in name_counts.items() if count > 1)  
        if duplicates:  
            raise ValueError(f"nomes de backends duplicados no config.json: {duplicates}")  
        return self  
  
  
def load_config(path: Path) -> GatewayConfig:  
    """Carrega e valida o ``config.json``, sempre levantando ``ValueError`` em falha.  
  
    Toda falha de leitura, parse ou validação é convertida em ``ValueError`` com  
    mensagem legível, de modo que o chamador (``main.py``) tenha um único tipo de  
    exceção para tratar e nunca receba um ``pydantic.ValidationError`` cru.  
    """  
    try:  
        raw: Any = json.loads(path.read_text(encoding="utf-8"))  
    except FileNotFoundError as exc:  
        raise ValueError(f"arquivo de configuração não encontrado: {path}") from exc  
    except json.JSONDecodeError as exc:  
        raise ValueError(f"config.json inválido em {path} (JSON malformado): {exc}") from exc  
    except UnicodeDecodeError as exc:  
        raise ValueError(f"config.json em {path} não é UTF-8 válido: {exc}") from exc  
    except OSError as exc:  
        raise ValueError(f"erro ao ler configuração em {path}: {exc}") from exc  
    try:  
        return GatewayConfig.model_validate(raw)  
    except ValidationError as exc:  
        raise ValueError(f"config.json inválido em {path}: {exc}") from exc