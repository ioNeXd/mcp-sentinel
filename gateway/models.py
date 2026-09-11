"""Modelos JSON-RPC 2.0 (request/response/error) e helpers de resposta."""

from typing import Any, Literal

from pydantic import BaseModel, model_validator

PROTOCOL_VERSION = "2024-11-05"
PROTOCOL_VERSIONS: frozenset[str] = frozenset([PROTOCOL_VERSION])
"""Versões de protocolo MCP aceitas pelo Gateway (conjunto revisável)."""

# Códigos de erro padrão do JSON-RPC 2.0.
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# Códigos de aplicação do Gateway na faixa -32000..-32099, reservada pela
# spec JSON-RPC 2.0 para erros definidos pelo servidor.
ITEM_NOT_FOUND = -32001
BACKEND_UNAVAILABLE = -32002


def negotiate_protocol_version(client_version: str | None) -> str | None:
    """Negocia a versão do protocolo MCP com um cliente.

    O spec MCP pede que o cliente envie uma versão no ``initialize`` e o
    servidor responda com a versão efetivamente negociada. O Gateway suporta
    o conjunto revisável ``PROTOCOL_VERSIONS``.

    Args:
        client_version: ``protocolVersion`` enviada pelo cliente (``None`` se
            o cliente não enviou o campo).

    Returns:
        A versão negociada (sempre suportada pelo Gateway) ou ``None`` quando
        a versão do cliente é incompatível.
    """
    if client_version is None:
        return PROTOCOL_VERSION
    if client_version in PROTOCOL_VERSIONS:
        return client_version
    return None


def is_supported_protocol_version(version: str | None) -> bool:
    """Indica se a versão de protocolo está no conjunto suportado."""
    if version is None:
        return False
    return version in PROTOCOL_VERSIONS


class JsonRpcRequest(BaseModel):
    """Request JSON-RPC 2.0 recebida pelo Gateway.

    O campo ``id`` tem três estados distintos na spec JSON-RPC 2.0:

    - **ausente** → notification (sem resposta);
    - **presente com valor ``null``** → request com id null (a resposta deve
      ter ``id: null``);
    - **presente com string/int** → request normal.

    Como ``None`` sozinho não diferencia "ausente" de "null explícito", o
    campo ``id_present`` guarda essa distinção — calculado no validador a
    partir do dict bruto.
    """

    jsonrpc: Literal["2.0"]
    id: int | str | None = None
    id_present: bool = False
    method: str
    params: dict[str, Any] | list[Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def _capture_id_presence(cls, data: Any) -> Any:
        """Registra a presença do campo ``id`` e rejeita ``id`` boolean.

        Em Python ``bool`` é subclasse de ``int``, então ``True``/``False``
        seriam coeridos para ``1``/``0`` e aceitos silenciosamente — mas a
        spec JSON-RPC só admite string, number ou null como id.
        """
        if isinstance(data, dict):
            data = dict(data)
            data["id_present"] = "id" in data
            if isinstance(data.get("id"), bool):
                raise ValueError("id não pode ser boolean (JSON true/false)")
        return data


class JsonRpcErrorDetail(BaseModel):
    """Corpo de um erro JSON-RPC (code/message/data)."""

    code: int
    message: str
    data: Any = None


class JsonRpcResponse(BaseModel):
    """Resposta JSON-RPC 2.0 (result OU error)."""

    jsonrpc: Literal["2.0"]
    id: int | str | None = None
    result: Any = None
    error: JsonRpcErrorDetail | None = None


def make_result(request_id: int | str | None, result: Any) -> dict[str, Any]:
    """Monta um dict de resposta JSON-RPC 2.0 com ``result``.

    ``id`` e ``result`` são sempre incluídos, inclusive quando valem ``None``
    (a spec exige ``id`` presente na resposta, e ``result: null`` é um
    resultado válido — distinto de "campo ausente").
    """
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def make_error(
    request_id: int | str | None,
    code: int,
    message: str,
    data: Any = None,
) -> dict[str, Any]:
    """Monta um dict de resposta JSON-RPC 2.0 com ``error``.

    ``id`` é sempre incluído (``null`` quando a request era desconhecida/não
    parseável). O campo ``data`` só entra no objeto de erro quando não é
    ``None``, preservando a semântica anterior de ``exclude_none``.
    """
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}
