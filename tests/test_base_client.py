"""Testes do contrato compartilhado em gateway/clients/base.py (Etapa 1).

Cobre lifecycle consistente, conclusão única de pending requests, limpeza em
timeout/cancelamento/stop, CancelledError nunca engolido e respostas
malformadas gerando ERRO (nunca resultado vazio).
"""

import asyncio
from typing import Any

import pytest

from gateway.clients.base import BaseClient
from gateway.errors import (
    BackendDisconnectedError,
    BackendError,
    BackendJsonRpcError,
    BackendStateConflictError,
    BackendTimeoutError,
)
from gateway.models import PROTOCOL_VERSION

GOOD_INITIALIZE = {
    "protocolVersion": PROTOCOL_VERSION,
    "capabilities": {"tools": {"listChanged": False}},
    "serverInfo": {"name": "fake", "version": "0.0.0"},
}


class DummyClient(BaseClient):
    """Client mínimo que exercita apenas o contrato da base."""

    def __init__(self, initialize_result: Any = GOOD_INITIALIZE) -> None:
        super().__init__()
        self.initialize_calls = 0
        self.initialized_notifications = 0
        self._initialize_result = initialize_result
        self._block_initialize: asyncio.Event | None = None

    async def start(self) -> None:
        self._begin_start()
        try:
            await self._initialize()
        except BaseException:
            await self.stop()
            raise
        self._mark_ready()

    async def stop(self) -> None:
        if self._begin_stop():
            self._fail_pending(BackendDisconnectedError("DummyClient: cliente encerrado"))
            self._mark_stopped()

    async def send_request(
        self, method: str, params: dict[str, Any] | None = None
    ) -> Any:
        if method == "initialize":
            self.initialize_calls += 1
            if self._block_initialize is not None:
                await self._block_initialize.wait()
            return self._initialize_result
        raise NotImplementedError(method)

    async def _send_notification(
        self, method: str, params: dict[str, Any] | None = None
    ) -> None:
        self.initialized_notifications += 1


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_completo_publica_capabilities_e_fica_ready() -> None:
    client = DummyClient()
    await client.start()
    assert client.state == "ready"
    assert client.is_ready
    assert client.capabilities == GOOD_INITIALIZE["capabilities"]
    assert client.initialized_notifications == 1  # notifications/initialized enviado
    await client.stop()


@pytest.mark.parametrize(
    "result",
    [
        None,
        "texto",
        {},  # sem protocolVersion nem capabilities
        {"protocolVersion": "1999-01-01", "capabilities": {}},  # versão incompatível
        {"protocolVersion": PROTOCOL_VERSION, "capabilities": None},  # caps inválidas
        {"protocolVersion": PROTOCOL_VERSION, "capabilities": ["tools"]},
    ],
)
@pytest.mark.asyncio
async def test_initialize_só_termina_apos_validacao_completa(result) -> None:
    client = DummyClient(initialize_result=result)
    with pytest.raises(BackendError):
        await client._initialize()
    # NADA publicado: capabilities vazias, nenhuma notificação enviada.
    assert client.capabilities == {}
    assert client.initialized_notifications == 0


@pytest.mark.asyncio
async def test_start_falhando_nao_fica_meio_de_pe() -> None:
    client = DummyClient(initialize_result={"protocolVersion": "errada", "capabilities": {}})
    with pytest.raises(BackendError):
        await client.start()
    assert client.capabilities == {}
    assert client.initialized_notifications == 0
    await client.stop()  # chamar stop() após falha de start() é seguro


@pytest.mark.asyncio
async def test_start_duas_vezes_levanta_conflito() -> None:
    client = DummyClient()
    await client.start()
    with pytest.raises(BackendStateConflictError):
        await client.start()
    await client.stop()


@pytest.mark.asyncio
async def test_stop_e_idempotente() -> None:
    client = DummyClient()
    await client.start()
    await client.stop()
    await client.stop()  # segunda chamada: no-op, sem erro
    assert client.state == "stopped"


@pytest.mark.asyncio
async def test_ensure_ready_rejeita_request_sem_handshake() -> None:
    client = DummyClient()
    with pytest.raises(BackendStateConflictError):
        client._ensure_ready("tools/list")


@pytest.mark.asyncio
async def test_initialize_concorrente_levanta_conflito() -> None:
    client = DummyClient()
    client._block_initialize = asyncio.Event()
    task = asyncio.create_task(client._initialize())
    await asyncio.sleep(0)  # deixa o primeiro entrar no send_request
    with pytest.raises(BackendStateConflictError):
        await client._initialize()
    client._block_initialize.set()
    await task


# ---------------------------------------------------------------------------
# Pending requests: conclusão única
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_pending_duplicado_levanta_conflito() -> None:
    client = DummyClient()
    client._register_pending(1)
    with pytest.raises(BackendStateConflictError):
        client._register_pending(1)


@pytest.mark.asyncio
async def test_resolve_pending_conclui_uma_unicamente() -> None:
    client = DummyClient()
    future = client._register_pending("req-1")
    assert client._resolve_pending("req-1", {"ok": True}) is True
    # Resposta tardia do mesmo id: NÃO conclui duas vezes.
    assert client._resolve_pending("req-1", {"outro": True}) is False
    assert future.result() == {"ok": True}
    assert "req-1" not in client._pending


@pytest.mark.asyncio
async def test_reject_pending_conclui_uma_unicamente() -> None:
    client = DummyClient()
    future = client._register_pending("req-1")
    exc = BackendJsonRpcError(code=-32603, message="boom")
    assert client._reject_pending("req-1", exc) is True
    assert client._reject_pending("req-1", BackendError("tarde")) is False
    with pytest.raises(BackendJsonRpcError):
        future.result()


@pytest.mark.asyncio
async def test_fail_pending_falha_todas_e_limpa() -> None:
    client = DummyClient()
    future = client._register_pending(1)
    client._register_pending(2)
    client._fail_pending(BackendDisconnectedError("caiu"))
    with pytest.raises(BackendDisconnectedError):
        future.result()
    assert client._pending == {}
    client._fail_pending(BackendDisconnectedError("de novo"))  # segunda: no-op seguro


@pytest.mark.asyncio
async def test_request_abandonada_resposta_tardia_e_descartada() -> None:
    """Timeout remove a pending; a resposta que chega depois não conclui nada."""
    client = DummyClient()
    future = client._register_pending("t-1")
    with pytest.raises(BackendTimeoutError):
        await asyncio.wait_for(
            client._await_response("t-1", future, 0.02, "tools/list"), 1.0
        )
    assert "t-1" not in client._pending
    # Resposta tardia: _apply_response acha pending inexistente → False.
    assert client._apply_response({"jsonrpc": "2.0", "id": "t-1", "result": {}}) is False


# ---------------------------------------------------------------------------
# _await_response: timeout, cancelamento (nunca engolido) e sucesso
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_await_response_timeout_vira_backendtimeout() -> None:
    client = DummyClient()
    future = client._register_pending("t-1")
    with pytest.raises(BackendTimeoutError):
        await client._await_response("t-1", future, 0.02, "tools/call")
    assert client._pending == {}


@pytest.mark.asyncio
async def test_await_response_cancelamento_nunca_e_engolido() -> None:
    client = DummyClient()
    future = client._register_pending("t-1")
    task = asyncio.create_task(client._await_response("t-1", future, 5.0, "tools/call"))
    await asyncio.sleep(0)  # deixa a task iniciar a espera
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert "t-1" not in client._pending  # cancelamento limpa a pending
    assert not future.done() or future.cancelled()


@pytest.mark.asyncio
async def test_await_response_devolve_resultado_correlacionado() -> None:
    client = DummyClient()
    future = client._register_pending(7)
    client._resolve_pending(7, {"tools": []})
    assert await client._await_response(7, future, 1.0, "tools/list") == {"tools": []}
    assert client._pending == {}


# ---------------------------------------------------------------------------
# _apply_response: respostas malformadas geram ERRO, nunca resultado vazio
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_response_ok_resolve_com_result() -> None:
    client = DummyClient()
    future = client._register_pending(1)
    message = {"jsonrpc": "2.0", "id": 1, "result": {"tools": [{"name": "x"}]}}
    assert client._apply_response(message) is True
    assert future.result() == {"tools": [{"name": "x"}]}


@pytest.mark.asyncio
async def test_apply_response_erro_jsonrpc_vira_backendjsonrpcerror() -> None:
    client = DummyClient()
    future = client._register_pending(1)
    message = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32601, "message": "não achou"}}
    assert client._apply_response(message) is True
    with pytest.raises(BackendJsonRpcError) as excinfo:
        future.result()
    assert excinfo.value.code == -32601


@pytest.mark.parametrize(
    "message",
    [
        {"jsonrpc": "1.0", "id": 1, "result": {}},  # envelope errado
        {"jsonrpc": "2.0", "id": 1},  # sem result E sem error (o bug do stdio)
    ],
)
@pytest.mark.asyncio
async def test_apply_response_malformada_falha_pending_com_erro(message) -> None:
    client = DummyClient()
    future = client._register_pending(1)
    assert client._apply_response(message) is True
    with pytest.raises(BackendError):  # ERRO de domínio — nunca result None/{}
        future.result()
    assert client._pending == {}


@pytest.mark.asyncio
async def test_apply_response_nao_dict_e_nao_resposta_devolvem_false() -> None:
    client = DummyClient()
    future = client._register_pending(1)
    assert client._apply_response("lixo") is False
    assert client._apply_response({"jsonrpc": "2.0", "method": "ping"}) is False  # notificação
    assert future.done() is False  # notificação não mexe em pending


@pytest.mark.asyncio
async def test_apply_response_id_desconhecido_devolve_false() -> None:
    client = DummyClient()
    assert client._apply_response({"jsonrpc": "2.0", "id": 999, "result": {}}) is False
