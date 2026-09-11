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
    """Re-registrar um backend substitui o conjunto anterior por inteiro.  
  
    Após o re-registro, a versão antiga do item some (``get`` devolve ``None``)  
    e apenas a nova permanece consultável.  
    """  
    registry = registry_cls()  
    registry.register("backend-a", [item])  
    other_item = dict(item)  
    other_item[id_key] = item[id_key] + "-v2"  
    registry.register("backend-a", [other_item])  
    assert len(registry.list_all()) == 1  
    assert registry.get(namespaced) is None  
    assert registry.get(f"backend-a.{other_item[id_key]}") is not None  
  
  
@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)  
def test_sobrescrita_mantem_outros_backends(registry_cls, item, namespaced, id_key) -> None:  
    """Re-registrar backend-a não toca no snapshot de backend-b."""  
    registry = registry_cls()  
    registry.register("backend-a", [item])  
    registry.register("backend-b", [item])  
    other_item = dict(item)  
    other_item[id_key] = item[id_key] + "-v2"  
    registry.register("backend-a", [other_item])  
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
    """Item inválido é rejeitado atomicamente: o snapshot anterior sobrevive."""  
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
  
  
@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)  
def test_duplicado_na_mesma_leva_e_rejeitado(  
    registry_cls, item, namespaced, id_key  
) -> None:  
    """Dois itens da MESMA listagem com o mesmo id: erro, sem sobrescrever.  
  
    Diferente do re-registro entre levas (sobrescrita intencional), colisão  
    dentro da mesma leva é bug do backend e vira BackendError — o snapshot  
    anterior permanece intacto.  
    """  
    registry = registry_cls()  
    registry.register("backend-a", [item])  
    duplicado = dict(item)  
    duplicado["description"] = "outro item com o mesmo id"  
    with pytest.raises(BackendError, match="duplicado"):  
        registry.register("backend-a", [item, duplicado])  
    assert registry.get(namespaced) is not None  
    assert len(registry.list_all()) == 1  
  
  
@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)  
def test_metadata_e_copia_defensiva(registry_cls, item, namespaced, id_key) -> None:  
    """Mutar o dict original depois do registro não afeta o snapshot.  
  
    Usa um campo aninhado (lista dentro de ``inputSchema``) para provar que a  
    cópia é PROFUNDA: uma cópia rasa compartilharia a lista ``fields`` e veria  
    a mutação posterior.  
    """  
    import copy  
  
    registry = registry_cls()  
    original = copy.deepcopy(item)  
    original["inputSchema"] = dict(original.get("inputSchema", {}), fields=["a"])  
    schema_snapshot = copy.deepcopy(original["inputSchema"])  
    registry.register("backend-a", [original])  
    entry = registry.get(namespaced)  
    assert entry is not None  
    original["inputSchema"]["fields"].append("mutado-depois")  
    original[id_key] = "renomeado-depois"  
    assert entry.metadata["inputSchema"] == schema_snapshot  
    assert entry.metadata[id_key] == item[id_key]  
  
  
@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)  
def test_identifier_com_ponto_e_rejeitado_para_nomes(  
    registry_cls, item, namespaced, id_key  
) -> None:  
    """'.' é o delimitador do namespace: rejeitado em tools/prompts.  
  
    Para URIs de resources é permitido (exceção documentada: URIs contêm '.'  
    legitimamente e a reversão lá é por remoção de prefixo).  
    """  
    registry = registry_cls()  
    com_ponto = dict(item)  
    com_ponto[id_key] = item[id_key] + ".com.sufixo"  
    if registry_cls is ResourceRegistry:  
        registry.register("backend-a", [com_ponto])  
        assert registry.get(f"backend-a.{com_ponto[id_key]}") is not None  
    else:  
        with pytest.raises(BackendError, match="namespace"):  
            registry.register("backend-a", [com_ponto])  
  
  
@pytest.mark.parametrize("registry_cls,item,namespaced,id_key", CASES)  
def test_identifier_com_espaco_e_rejeitado(  
    registry_cls, item, namespaced, id_key  
) -> None:  
    """Espaço em branco no identificador vira BackendError claro."""  
    registry = registry_cls()  
    com_espaco = dict(item)  
    com_espaco[id_key] = item[id_key] + " com espaço"  
    with pytest.raises(BackendError, match="espaço"):  
        registry.register("backend-a", [com_espaco])