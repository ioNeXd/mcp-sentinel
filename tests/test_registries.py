"""Testes unitários dos três registries (Fase 1).

Cada caso parametrizado roda contra os três registries com um item típico:
registro, sobrescrita de um mesmo backend, remoção, não-colisão de namespace
e rejeição de itens inválidos sem corromper o snapshot atual.
"""

import pytest

from gateway.errors import BackendError
from gateway.registries import PromptRegistry, ResourceRegistry, ToolRegistry

TOOL_ITEM = {"name": "echo", "description": "Repete.", "inputSchema": {"type": "object"}}
RESOURCE_ITEM = {
    "uri": "file:///tmp/a.txt",
    "name": "a.txt",
    "mimeType": "text/plain",
}
PROMPT_ITEM = {"name": "greet", "description": "Sauda.", "arguments": []}

# (registry, item, namespaced esperado, chave do identificador)
CASES = [
    (ToolRegistry, TOOL_ITEM, "backend-a.echo", "name"),
    (ResourceRegistry, RESOURCE_ITEM, "backend-a.file:///tmp/a.txt", "uri"),
    (PromptRegistry, PROMPT_ITEM, "backend-a.greet", "name"),
]


@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)
def test_register_get_list(registry_cls, item, namespaced, id_key) -> None:
    registry = registry_cls()
    registry.register("backend-a", [item])
    entries = registry.list_all()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.backend == "backend-a"
    assert entry.name == item[id_key]
    assert entry.namespaced == namespaced
    assert entry.metadata == item
    assert registry.get(namespaced) is entry
    assert registry.get("nao-existe") is None


@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)
def test_dois_backends_nao_colidem(registry_cls, item, namespaced, id_key) -> None:
    """Mesmo item em dois backends coexiste graças ao prefixo de namespace."""
    registry = registry_cls()
    registry.register("backend-a", [item])
    registry.register("backend-b", [item])
    assert len(registry.list_all()) == 2
    assert registry.get(f"backend-a.{item[id_key]}") is not None
    assert registry.get(f"backend-b.{item[id_key]}") is not None


@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)
def test_sobrescrita_de_mesmo_backend_nao_deixa_sobras(
    registry_cls, item, namespaced, id_key
) -> None:
    """Re-registrar um backend substitui o conjunto anterior por inteiro."""
    registry = registry_cls()
    registry.register("backend-a", [item])
    other_item = dict(item)
    other_item[id_key] = item[id_key] + "-v2"
    registry.register("backend-a", [other_item])
    assert len(registry.list_all()) == 1
    assert registry.get(namespaced) is None  # versão antiga sumiu
    assert registry.get(f"backend-a.{other_item[id_key]}") is not None


@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)
def test_sobrescrita_mantem_outros_backends(registry_cls, item, namespaced, id_key) -> None:
    registry = registry_cls()
    registry.register("backend-a", [item])
    registry.register("backend-b", [item])
    other_item = dict(item)
    other_item[id_key] = item[id_key] + "-v2"
    registry.register("backend-a", [other_item])
    # backend-b permanece intacto após re-registro de backend-a.
    assert registry.get(f"backend-b.{item[id_key]}") is not None


@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)
def test_unregister_remove_apenas_o_backend(registry_cls, item, namespaced, id_key) -> None:
    registry = registry_cls()
    registry.register("backend-a", [item])
    registry.register("backend-b", [item])
    registry.unregister("backend-a")
    assert registry.get(namespaced) is None
    assert registry.get(f"backend-b.{item[id_key]}") is not None
    assert len(registry.list_all()) == 1


@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)
def test_item_invalido_e_rejeitado_sem_corromper_snapshot(registry_cls, item, namespaced, id_key) -> None:
    registry = registry_cls()
    registry.register("backend-a", [item])
    bad_items: list[object] = [
        {},
        {id_key: ""},
        {id_key: 123},
        "nao-e-dict",
    ]
    for bad in bad_items:
        with pytest.raises(BackendError, match="backend-a"):
            registry.register("backend-a", [bad])  # type: ignore[list-item]
        # Registro anterior permanece intacto (falha é atômica).
        assert registry.get(namespaced) is not None
        assert len(registry.list_all()) == 1


def test_registry_instancias_independentes() -> None:
    """ToolRegistry/ResourceRegistry/PromptRegistry não compartilham estado."""
    tools = ToolRegistry()
    resources = ResourceRegistry()
    tools.register("backend-a", [TOOL_ITEM])
    assert resources.list_all() == []
    assert resources.get("backend-a.echo") is None
    assert PromptRegistry().get("backend-a.echo") is None
