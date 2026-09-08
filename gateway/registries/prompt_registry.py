"""PromptRegistry: registro agregado de prompts expostos pelos backends."""

from gateway.registries.base import BaseRegistry


class PromptRegistry(BaseRegistry):
    """Prompts expostos com namespace ``backend.nome_do_prompt``.

    O item registrado é o dict ``prompts/list`` do backend (com ``name``,
    ``description`` e ``arguments``); o campo identificador é ``name``.
    """

    _IDENTIFIER_KEY = "name"
