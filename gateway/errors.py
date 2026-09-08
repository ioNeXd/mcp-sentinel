"""Exceções do domínio do Gateway."""

from typing import Any


class BackendError(Exception):
    """Erro genérico de comunicação com um backend MCP."""


class BackendTimeoutError(BackendError):
    """Backend não respondeu dentro do timeout configurado."""


class BackendDisconnectedError(BackendError):
    """Processo do backend caiu ou foi encerrado."""


class BackendJsonRpcError(BackendError):
    """Backend respondeu com um erro JSON-RPC (carrega code/message/data)."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data