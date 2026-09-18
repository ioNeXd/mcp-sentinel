"""Testes do broadcaster de logs — foco em ``_json_safe``.

``_json_safe`` é a fronteira entre o event_dict do structlog e o SSE do
console ao vivo: o ``JSON.parse`` do dashboard precisa receber objeto/array
navegável para contexto estruturado, não a repr Python em string.
"""

import json
from datetime import datetime

from gateway.log_stream import _MAX_JSON_SAFE_DEPTH, _json_safe


def test_contexto_aninhado_chega_como_objeto_json_navegavel() -> None:
    """dict/list aninhados viram estrutura, não repr — caso real: per_backend
    do diagnóstico de tools (item 20 da revisão)."""
    event = _json_safe(
        {
            "event": "tools_list_size",
            "per_backend_chars": {"backend-a": 93, "backend-b": 84},
            "itens": [{"uri": "fs/a.txt", "size": 93}],
            "count": 2,
        }
    )

    assert event["per_backend_chars"] == {"backend-a": 93, "backend-b": 84}
    assert event["itens"] == [{"uri": "fs/a.txt", "size": 93}]
    assert event["count"] == 2
    # O payload inteiro serializa como JSON de verdade (era isso que o front
    # recebia como string de repr antes da correção).
    decoded = json.loads(json.dumps(event, ensure_ascii=False))
    assert decoded["per_backend_chars"] == {"backend-a": 93, "backend-b": 84}


def test_primitivos_passam_direto() -> None:
    event = _json_safe({"event": "x", "ts": 3, "ratio": 1.5, "ok": True, "nada": None})

    assert event["ts"] == 3
    assert event["ratio"] == 1.5
    assert event["ok"] is True
    assert event["nada"] is None


def test_folhas_exoticas_viram_str() -> None:
    """datetime e outros objetos sem representação JSON caem no str() —
    comportamento de fallback preservado da versão de 1 nível."""
    event = _json_safe({"quando": datetime(2026, 1, 1)})

    assert isinstance(event["quando"], str)
    assert "2026" in event["quando"]


def test_sets_viram_array() -> None:
    """set/frozenset/tuple não existem em JSON: viram array (antes era str(set))."""
    event = _json_safe({"tags": {"a", "b"}, "par": (1, 2)})

    assert sorted(event["tags"]) == ["a", "b"]
    assert event["par"] == [1, 2]


def test_referencia_circular_nao_explode() -> None:
    """Um log quebrado não pode derrubar o logging (ver LogBroadcaster.publish):
    a recursão tem teto — ao atingi-lo, o valor vira str()."""
    cyclic: dict[str, object] = {"name": "loop"}
    cyclic["self"] = cyclic

    event = _json_safe({"c": cyclic})

    node = event["c"]
    for _ in range(_MAX_JSON_SAFE_DEPTH):
        assert isinstance(node, dict)  # dentro do teto: objeto navegável
        node = node["self"]
    assert isinstance(node, str)  # além do teto: string, sem RecursionError


def test_chaves_nao_string_sao_coagidas() -> None:
    """JSON exige chaves string; int/None como chave viram str na conversão."""
    event = _json_safe({1: "a", None: "b", "x": 1})

    assert event["1"] == "a"
    assert event["None"] == "b"
    assert event["x"] == 1


def test_sem_injecao_de_timestamp_ou_level() -> None:
    """_json_safe é pass-through: NÃO inventa timestamp/level ausentes.

    Contrato (item 32): na cadeia real o broadcast_processor roda DEPOIS de
    ``add_log_level`` e ``TimeStamper(fmt="iso")``, então os dois campos já
    chegam preenchidos (level string, timestamp ISO-8601 string). Os
    ``setdefault`` antigos eram código morto — e o default epoch de timestamp
    sugeria um formato que o pipeline não produz. Quem chamar sem os campos
    (só possível fora do pipeline) recebe o dict tal como veio.
    """
    event = _json_safe({})
    assert event == {}

    partial = _json_safe({"level": "error"})
    assert partial == {"level": "error"}
    assert "timestamp" not in partial
