"""Modelos JSON-RPC 2.0 (request/response/error) e helpers de resposta."""

from typing import Any, Literal

from pydantic import BaseModel, model_validator

# Códigos de erro padrão do JSON-RPC 2.0.
PROTOCOL_VERSION = "2024-11-05"
# Versões de protocolo MCP suportadas pelo Gateway (conjunto revisável).
PROTOCOL_VERSIONS: frozenset[str] = frozenset([PROTOCOL_VERSION])
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# Códigos customizados de aplicação do Gateway (dentro da faixa -32000..-32099,
# reservada pelo JSON-RPC 2.0 para erros definidos pelo servidor).
ITEM_NOT_FOUND = -32001  # tool/resource/prompt namespaced não encontrado
BACKEND_UNAVAILABLE = -32002  # backend indisponível/processo morto durante a chamada


def negotiate_protocol_version(client_version: str | None) -> str | None:
    """Negocia a versão do protocolo MCP com um cliente.

    O spec MCP pede que o cliente envie uma versão de protocolo no
    ``initialize`` e o servidor responda com a versão que efetivamente
    negociou. O Gateway suporta um conjunto revisável de versões
    (``PROTOCOL_VERSIONS``).

    Args:
        client_version: ``protocolVersion`` enviada pelo cliente (pode ser
            ``None`` se o cliente não enviou o campo).

    Returns:
        A versão negociada (sempre uma string suportada pelo Gateway) ou
        ``None`` quando a versão do cliente não é compatível.
    """
    if client_version is None:
        return PROTOCOL_VERSION
    if client_version in PROTOCOL_VERSIONS:
        return client_version
    return None


def is_supported_protocol_version(version: str | None) -> bool:
    """Verifica se uma versão de protocolo está no conjunto suportado."""
    if version is None:
        return False
    return version in PROTOCOL_VERSIONS


class JsonRpcRequest(BaseModel):
    """Request JSON-RPC 2.0 recebida pelo Gateway.

    O campo ``id`` tem três estados distintos pela spec JSON-RPC 2.0:
    - **ausente** → notification (sem resposta);
    - **presente com valor ``null``** → request com id null (a resposta deve
      ter ``id: null``);
    - **presente com string/int** → request normal.

    A distinção entre "ausente" e "null explícito" é feita pelo campo
    ``id_present`` (calculado no validador a partir do dict bruto), já que
    ``None`` sozinho não diferencia os dois casos.
    """

    jsonrpc: Literal["2.0"]
    id: int | str | None = None
    id_present: bool = False
    method: str
    params: dict[str, Any] | list[Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def _capture_id_presence(cls, data: Any) -> Any:
        """Registra se o campo ``id`` estava presente no JSON (incluso null).

        Também rejeita ``id`` boolean: em Python ``bool`` é subclass de
        ``int``, então ``True``/``False`` seriam coeridos para ``1``/``0``
        e aceitos silenciosamente — a spec JSON-RPC só permite string,
        number ou null.
        """
        if isinstance(data, dict):
            data = dict(data)
            data["id_present"] = "id" in data
            if isinstance(data.get("id"), bool):
                raise ValueError("id não pode ser boolean (JSON true/false)")
        return data


class JsonRpcErrorDetail(BaseModel):
    """Corpo de um erro JSON-RPC."""

    code: int
    message: str
    data: Any = None


class JsonRpcResponse(BaseModel):
    """Resposta JSON-RPC 2.0 (result ou error)."""

    jsonrpc: Literal["2.0"]
    id: int | str | None = None
    result: Any = None
    error: JsonRpcErrorDetail | None = None


def make_result(request_id: int | str | None, result: Any) -> dict[str, Any]:
    """Monta um dict de resposta JSON-RPC com resultado."""
    payload = JsonRpcResponse(
        jsonrpc="2.0", id=request_id, result=result
    ).model_dump(exclude_none=True)
    payload["result"] = result
    if request_id is None:
        payload["id"] = None
    return payload


def make_error(
    request_id: int | str | None,
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