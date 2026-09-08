"""Modelos JSON-RPC 2.0 (request/response/error) e helpers de resposta."""

from typing import Any, Literal

from pydantic import BaseModel

# Códigos de erro padrão do JSON-RPC 2.0.
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# Códigos customizados de aplicação do Gateway (dentro da faixa -32000..-32099,
# reservada pelo JSON-RPC 2.0 para erros definidos pelo servidor).
ITEM_NOT_FOUND = -32001  # tool/resource/prompt namespaced não encontrado
BACKEND_UNAVAILABLE = -32002  # backend indisponível/processo morto durante a chamada

JsonRpcId = int | str


class JsonRpcRequest(BaseModel):
    """Request JSON-RPC 2.0 recebido pelo Gateway."""

    jsonrpc: Literal["2.0"]
    id: JsonRpcId | None = None
    method: str
    params: dict[str, Any] | list[Any] | None = None


class JsonRpcErrorDetail(BaseModel):
    """Corpo de um erro JSON-RPC."""

    code: int
    message: str
    data: Any = None


class JsonRpcResponse(BaseModel):
    """Resposta JSON-RPC 2.0 (result ou error)."""

    jsonrpc: Literal["2.0"]
    id: JsonRpcId | None = None
    result: Any = None
    error: JsonRpcErrorDetail | None = None


def make_result(request_id: JsonRpcId | None, result: Any) -> dict[str, Any]:
    """Monta um dict de resposta JSON-RPC com resultado."""
    return JsonRpcResponse(jsonrpc="2.0", id=request_id, result=result).model_dump(exclude_none=True)


def make_error(
    request_id: JsonRpcId | None,
    code: int,
    message: str,
    data: Any = None,
) -> dict[str, Any]:
    """Monta um dict de resposta JSON-RPC com erro (id null quando desconhecido)."""
    payload = JsonRpcResponse(
        jsonrpc="2.0",
        id=request_id,
        error=JsonRpcErrorDetail(code=code, message=message, data=data),
    ).model_dump(exclude_none=True)
    if request_id is None:
        payload["id"] = None
    return payload