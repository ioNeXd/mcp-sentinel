"""Testes dos envelopes JSON-RPC."""

from gateway.models import JsonRpcRequest, make_result


def test_make_result_preserva_result_null() -> None:
    payload = make_result(1, None)
    assert payload["result"] is None
    assert payload["id"] == 1


def test_request_distingue_id_ausente_de_null_explicito() -> None:
    notification = JsonRpcRequest.model_validate({"jsonrpc": "2.0", "method": "ping"})
    null_request = JsonRpcRequest.model_validate({"jsonrpc": "2.0", "id": None, "method": "ping"})
    identified_request = JsonRpcRequest.model_validate(
        {"jsonrpc": "2.0", "id": 1, "method": "ping"}
    )

    assert notification.id_present is False
    assert null_request.id_present is True
    assert identified_request.id_present is True
