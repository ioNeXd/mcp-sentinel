"""Base comum dos registries agregados.

Todos os registries guardam itens vindos de N backends num dict chaveado pelo
identificador *namespaced* (``backend.<id original>``). A atualização nunca
muta o dict em uso: ``register``/``unregister`` constroem um dict novo e fazem
o swap inteiro por atribuição (``self._items = novo``), que é atômico sob o
GIL. Leitores concorrentes enxergam sempre um snapshot consistente — o mesmo
padrão que a Fase 2 reaproveita para recarregar registries na troca de um
backend sem travar requisições em andamento.
"""

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
        namespaced: Identificador exposto pelo Gateway.
        metadata: Dict original completo enviado pelo backend.
    """

    backend: str
    name: str
    namespaced: str
    metadata: dict[str, Any]


class BaseRegistry:
    """Registro de itens de N backends com namespace ``backend.<id>``.

    Subclasses definem em qual campo do item o backend entrega o
    identificador (``name`` para tools/prompts, ``uri`` para resources).
    """

    _IDENTIFIER_KEY: ClassVar[str] = "name"

    def __init__(self) -> None:
        self._items: dict[str, RegistryEntry] = {}

    def register(self, backend_name: str, items: list[dict[str, Any]]) -> None:
        """(Re)registra os itens de um backend num único swap do snapshot.

        Substitui integralmente os itens que já existiam daquele backend
        (sobrescrita sem sobras). Levanta BackendError se algum item for
        inválido — nesse caso o snapshot anterior permanece intacto.
        """
        new_items = {
            namespaced: entry
            for namespaced, entry in self._items.items()
            if entry.backend != backend_name
        }
        for item in items:
            entry = self._build_entry(backend_name, item)
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
        return RegistryEntry(
            backend=backend_name,
            name=name,
            namespaced=f"{backend_name}.{name}",
            metadata=item,
        )
