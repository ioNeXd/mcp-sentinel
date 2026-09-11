"""Base comum dos registries agregados.  
  
Todos os registries guardam itens vindos de N backends num dict chaveado pelo  
identificador *namespaced* (``backend.<id original>``). A atualização nunca  
muta o dict em uso: ``register``/``unregister`` constroem um dict novo e fazem  
o swap inteiro por atribuição (``self._items = novo``), que é atômico sob o  
GIL. Leitores concorrentes enxergam sempre um snapshot consistente — o mesmo  
padrão que a troca de um backend reaproveita para recarregar registries sem  
travar requisições em andamento.  
"""  
  
import copy  
from dataclasses import dataclass  
from typing import Any, ClassVar  
  
from gateway.errors import BackendError  
  
  
@dataclass(frozen=True)  
class RegistryEntry:  
    """Item registrado proveniente de um backend.  
  
    Attributes:  
        backend: Nome do backend de origem.  
        name: Identificador original do item no backend (nome de tool,  
            URI de resource ou nome de prompt).  
        namespaced: Identificador exposto pelo Gateway (``backend.<id>``).  
        metadata: Cópia defensiva (deepcopy) do dict enviado pelo backend —  
            o registry não compartilha referência com o chamador, então  
            mutações no dict original nunca afetam um snapshot já publicado.  
    """  
  
    backend: str  
    name: str  
    namespaced: str  
    metadata: dict[str, Any]  
  
  
class BaseRegistry:  
    """Registro de itens de N backends com namespace ``backend.<id>``.  
  
    Subclasses definem em qual campo do item o backend entrega o  
    identificador (``name`` para tools/prompts, ``uri`` para resources) via  
    ``_IDENTIFIER_KEY``.  
  
    ``_ALLOW_NAMESPACE_SEPARATOR`` controla se o identificador original pode  
    conter ``.`` — o delimitador do esquema de namespace ``backend.<id>``.  
    Para tools/prompts é ``False`` (um ``.`` no nome tornaria o identificador  
    namespaced ambíguo); resources são a exceção documentada (ver  
    ``ResourceRegistry``), pois URIs contêm ``.`` legitimamente e a reversão  
    do namespace é feita por remoção do prefixo ``backend.``, não por split.  
    """  
  
    _IDENTIFIER_KEY: ClassVar[str] = "name"  
    _ALLOW_NAMESPACE_SEPARATOR: ClassVar[bool] = False  
  
    def __init__(self) -> None:  
        self._items: dict[str, RegistryEntry] = {}  
  
    def register(self, backend_name: str, items: list[dict[str, Any]]) -> None:  
        """(Re)registra os itens de um backend num único swap do snapshot.  
  
        Substitui integralmente os itens que já existiam daquele backend  
        (sobrescrita sem sobras — re-registro intencional do mesmo backend).  
        Levanta BackendError se algum item for inválido ou se dois itens da  
        MESMA listagem resolverem para o mesmo identificador namespaced  
        (colisão dentro da leva — diferente da sobrescrita entre levas, que  
        é o mecanismo normal de atualização). Em qualquer falha, o snapshot  
        anterior permanece intacto: o swap só acontece no fim, sem exceções.  
        """  
        new_items = {  
            namespaced: entry  
            for namespaced, entry in self._items.items()  
            if entry.backend != backend_name  
        }  
        seen: set[str] = set()  
        for item in items:  
            entry = self._build_entry(backend_name, item)  
            if entry.namespaced in seen:  
                raise BackendError(  
                    f"backend '{backend_name}': identificador duplicado na mesma"  
                    f" listagem: '{entry.name}' (namespaced '{entry.namespaced}')"  
                )  
            seen.add(entry.namespaced)  
            new_items[entry.namespaced] = entry  
        self._items = new_items  
  
    def unregister(self, backend_name: str) -> None:  
        """Remove todos os itens de um backend num único swap do snapshot."""  
        self._items = {  
            namespaced: entry  
            for namespaced, entry in self._items.items()  
            if entry.backend != backend_name  
        }  
  
    def get(self, namespaced: str) -> RegistryEntry | None:  
        """Devolve a entrada correspondente ao identificador namespaced."""  
        return self._items.get(namespaced)  
  
    def list_all(self) -> list[RegistryEntry]:  
        """Lista as entradas atuais, na ordem de registro."""  
        return list(self._items.values())  
  
    def _build_entry(self, backend_name: str, item: Any) -> RegistryEntry:  
        """Valida um item cru do backend e constrói a entrada namespaced.  
  
        O item precisa ser um dict com ``_IDENTIFIER_KEY`` mapeando para uma  
        string não vazia. ``metadata`` é uma cópia profunda do dict original:  
        payloads têm listas/dicts aninhados (inputSchema, arguments etc.) e  
        uma referência compartilhada permitiria que o dono do dict original  
        mutasse um snapshot já publicado.  
        """  
        if (  
            not isinstance(item, dict)  
            or not isinstance(item.get(self._IDENTIFIER_KEY), str)  
            or not item[self._IDENTIFIER_KEY]  
        ):  
            raise BackendError(  
                f"backend '{backend_name}': item sem campo '{self._IDENTIFIER_KEY}'"  
                f" (string não vazia): {item!r}"  
            )  
        name = item[self._IDENTIFIER_KEY]  
        self._validate_identifier(backend_name, name)  
        return RegistryEntry(  
            backend=backend_name,  
            name=name,  
            namespaced=f"{backend_name}.{name}",  
            metadata=copy.deepcopy(item),  
        )  
  
    def _validate_identifier(self, backend_name: str, identifier: str) -> None:  
        """Valida o identificador original vindo do backend.  
  
        Regras de domínio: sem espaço em branco (o nome vira parte de um  
        identificador exposto; backend_name tem a mesma regra documentada no  
        README) e, para tools/prompts, sem ``.`` — o delimitador do próprio  
        esquema de namespace ``backend.<id>`` (resources abrem exceção via  
        ``_ALLOW_NAMESPACE_SEPARATOR``).  
        """  
        if any(ch.isspace() for ch in identifier):  
            raise BackendError(  
                f"backend '{backend_name}': identificador inválido"  
                f" ({self._IDENTIFIER_KEY} não pode conter espaço): {identifier!r}"  
            )  
        if "." in identifier and not self._ALLOW_NAMESPACE_SEPARATOR:  
            raise BackendError(  
                f"backend '{backend_name}': identificador inválido"  
                f" ({self._IDENTIFIER_KEY} não pode conter '.', delimitador do"  
                f" namespace): {identifier!r}"  
            )