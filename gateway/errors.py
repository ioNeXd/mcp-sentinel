"""Exceções do domínio do Gateway."""

from typing import Any


class BackendError(Exception):
    """Erro genérico de comunicação com um backend MCP."""


class BackendNotFoundError(BackendError):
    """Backend não existe na configuração do Gateway."""


class BackendStateConflictError(BackendError):
    """Operação incompatível com o estado atual do backend."""


class BackendTimeoutError(BackendError):
    """Backend não respondeu dentro do timeout configurado."""


class BackendDisconnectedError(BackendError):
    """Processo do backend caiu ou foi encerrado."""

    def __init__(self, message: str, *, backend: str | None = None) -> None:
        super().__init__(message)
        self.backend = backend


class BackendHttpStatusError(BackendDisconnectedError):
    """Backend respondeu HTTP com código de erro (≥400).

    Subclasse de ``BackendDisconnectedError`` para compatibilidade com
    quem trata o tipo genérico — mas permite, no futuro, diferenciar
    programaticamente uma resposta 4xx (ex.: 401) de uma falha de rede.
    """

    def __init__(self, status_code: int, method: str, *, backend: str | None = None) -> None:
        message = ("backend '" + (backend or "backend desconhecido") + "': HTTP " +
                   str(status_code) + " em '" + method + "'")
        super().__init__(message, backend=backend)
        self.status_code = status_code
        self.method = method


class BackendJsonRpcError(BackendError):
    """Backend respondeu com um erro JSON-RPC (carrega code/message/data)."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data