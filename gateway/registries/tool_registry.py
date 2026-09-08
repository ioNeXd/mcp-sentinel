"""ToolRegistry: registro agregado de tools expostas pelos backends."""

from gateway.registries.base import BaseRegistry


class ToolRegistry(BaseRegistry):
    """Tools expostas com namespace ``backend.nome_da_tool``.

    O item registrado é o dict ``tools/list`` do backend (com ``name``,
    ``description`` e ``inputSchema``); o campo identificador é ``name``.
    """

    _IDENTIFIER_KEY = "name"
