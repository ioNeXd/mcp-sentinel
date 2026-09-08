"""Testes dos envelopes JSON-RPC."""

from gateway.models import make_result


def test_make_result_preserva_result_null() -> None:
    payload = make_result(1, None)
    assert payload["result"] is None
    assert payload["id"] == 1
