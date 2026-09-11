"""ResourceRegistry: registro agregado de resources expostos pelos backends.

Decisão de namespace para URIs
------------------------------
Um resource é endereçado por URI (``resources/read`` recebe ``uri``), então o
namespace não pode virar um "nome" separado como nas tools. Aplicamos o mesmo
prefixo usado em tudo: o identificador namespaced vira ``backend.<uri>`` — por
exemplo, ``file:///tmp/a.txt`` do backend ``fs`` é exposto como
``fs.file:///tmp/a.txt``. Isso produz uma string que continua sendo uma URI
válida (o prefixo vira parte do scheme, e ``.`` é um caractere permitido em
schemes), é reversível de forma trivial (basta remover o prefixo
``backend.``) e mantém o registro uniforme com tools/prompts. Não usamos um
parâmetro separado (ex.: ``?backend=``) porque isso poluiria URIs opacas e
quebraria resources não-hierárquicos; não reescrevemos o path porque backends
diferentes podem servir URIs idênticas e a origem precisa ser resolúvel só
pela string exposta.
"""

from gateway.registries.base import BaseRegistry


class ResourceRegistry(BaseRegistry):
    """Resources expostos com namespace ``backend.<uri original>``.

    O item registrado é o dict ``resources/list`` do backend (com ``uri``,
    ``name``, ``description`` e ``mimeType``); o campo identificador é ``uri``.

    ``_ALLOW_NAMESPACE_SEPARATOR`` é ``True`` porque URIs contêm ``.``
    legitimamente (ex.: ``file:///tmp/a.txt``): a regra que rejeita o
    delimitador vale só para tools/prompts. A reversão do namespace aqui é
    feita por remoção do prefixo ``backend.`` — nomes de backend não contêm
    pontos, então a separação é inequívoca (ver docstring do módulo) — e não
    por ``split`` no primeiro ponto.
    """

    _IDENTIFIER_KEY = "uri"
    _ALLOW_NAMESPACE_SEPARATOR = True
