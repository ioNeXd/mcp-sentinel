"""Testes do controle manual de backends no BackendManager (Fase 4).

Cobre disable/enable/restart nos três eixos exigidos: estado e registries,
interação com o Health Monitor (disabled é intocável) e propagação de erro
para as rotas HTTP (404/409/503 decididas lá).
"""

import asyncio

import pytest

from conftest import FakeClient, capture_structlog_events, make_fake_manager
from gateway.backend_manager import BackendManager, BackendStatus
from gateway.errors import BackendError

ECHO_TOOL = {
    "name": "echo",
    "description": "Repete texto.",
    "inputSchema": {"type": "object", "properties": {}},
}


def zero_backoff(manager: BackendManager) -> None:
    manager._backoff_seconds = lambda consecutive_failures: 0.0  # type: ignore[method-assign]


async def drain_restarts(manager: BackendManager) -> None:
    pending = [t for t in manager._restart_tasks.values() if not t.done()]
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


# ----------------------------------------------------------------------
# disable
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disable_para_client_e_limpa_registries() -> None:
    manager, factory = make_fake_manager(("backend-a",))
    await manager.start_all()
    try:
        await manager.disable("backend-a")
        state = manager.get_state("backend-a")
        assert state.status is BackendStatus.DISABLED
        assert factory.created[0].stopped  # client foi parado de verdade
        assert state.client is None
        assert manager.registries[0].list_all() == []  # tools saem do registry
        assert manager.registries[1].list_all() == []
        assert manager.registries[2].list_all() == []
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_disable_e_idempotente() -> None:
    manager, factory = make_fake_manager(("backend-a",))
    await manager.start_all()
    try:
        await manager.disable("backend-a")
        await manager.disable("backend-a")  # segunda chamada não faz nada
        assert manager.status_of("backend-a") is BackendStatus.DISABLED
        assert len(factory.created) == 1  # nenhum client novo foi criado
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_disable_backend_inexistente_levanta_erro() -> None:
    manager, _ = make_fake_manager(("backend-a",))
    await manager.start_all()
    try:
        with pytest.raises(BackendError, match="não existe"):
            await manager.disable("fantasma")
    finally:
        await manager.stop_all()


# ----------------------------------------------------------------------
# Health Monitor x disabled
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monitor_nao_toca_backend_disabled() -> None:
    """Backend disabled permanece disabled mesmo 'caído' — restart é erro aqui.

    Regra da Fase 4: desligamento intencional ≠ falha. O monitor não checa
    saúde, não marca offline e não agenda restart; loga o aviso uma única vez
    (não a cada ciclo).
    """
    manager, factory = make_fake_manager(("backend-a",))
    zero_backoff(manager)
    await manager.start_all()
    try:
        client = factory.created[0]
        await manager.disable("backend-a")
        # Simula "cairia": o client parou, como um processo morto.
        assert client.stopped

        with capture_structlog_events() as events:
            for _ in range(3):  # vários ciclos do monitor
                status = await manager.check_and_recover("backend-a")
                await drain_restarts(manager)
        assert status is BackendStatus.DISABLED
        assert manager.status_of("backend-a") is BackendStatus.DISABLED
        # Nada foi recriado: nenhuma tentativa de restart, nenhum client novo.
        assert len(factory.created) == 1
        assert manager.registries[0].list_all() == []
        # Aviso logado exatamente UMA vez, não a cada ciclo.
        warnings = [e for e in events if e.get("event") == "backend_disabled_ignorado_pelo_monitor"]
        assert len(warnings) == 1
        assert warnings[0]["backend"] == "backend-a"
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_disable_cancela_restart_pendente() -> None:
    """disable() durante um restart agendado: a task é cancelada, não conflita."""
    manager, factory = make_fake_manager(("backend-a",))
    manager._backoff_seconds = lambda failures: 5.0  # type: ignore[method-assign]
    await manager.start_all()
    try:
        state = manager.get_state("backend-a")
        await state.client.stop()  # cai
        await manager.check_and_recover("backend-a")  # agenda restart (backoff 5s)
        pending = manager._restart_tasks.get("backend-a")
        assert pending is not None and not pending.done()

        await manager.disable("backend-a")  # cancela o restart pendente
        assert pending.cancelled()
        assert manager._restart_tasks.get("backend-a") is None
        assert manager.status_of("backend-a") is BackendStatus.DISABLED

        # A task cancelada não "recupera" o backend depois do disable.
        await asyncio.sleep(0.01)
        assert manager.status_of("backend-a") is BackendStatus.DISABLED
        assert manager.registries[0].list_all() == []
    finally:
        await manager.stop_all()


# ----------------------------------------------------------------------
# enable
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enable_reintegra_backend_com_tools_de_volta() -> None:
    manager, factory = make_fake_manager(("backend-a",))
    zero_backoff(manager)
    await manager.start_all()
    try:
        await manager.disable("backend-a")
        await manager.enable("backend-a")
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        # Client NOVO foi criado (não é o mesmo que foi parado no disable).
        assert len(factory.created) == 2
        assert factory.created[-1].started
        assert [e.namespaced for e in manager.registries[0].list_all()] == ["backend-a.echo"]
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_enable_em_backend_nao_disabled_levanta_erro() -> None:
    """enable só vale para disabled — rota HTTP traduz isto em 409."""
    manager, _ = make_fake_manager(("backend-a",))
    await manager.start_all()
    try:
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        with pytest.raises(BackendError, match="enable só se aplica"):
            await manager.enable("backend-a")
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_enable_falhando_propaga_erro_e_deixa_offline() -> None:
    """Se o backend não sobe no enable, a rota precisa saber (503).

    Diferente do restart automático (fire-and-forget), enable/restart manuais
    propagam a falha; o backend fica offline e volta ao ciclo normal do
    Health Monitor.
    """
    manager, factory = make_fake_manager(("backend-a",))
    await manager.start_all()
    try:
        await manager.disable("backend-a")
        # Próximo client a ser criado falha ao subir.
        manager._create_client = lambda backend_config: FakeClient(start_error=True)  # type: ignore[method-assign,return-value]
        with pytest.raises(BackendError, match="não subiu após enable"):
            await manager.enable("backend-a")
        assert manager.status_of("backend-a") is BackendStatus.OFFLINE
        assert manager.registries[0].list_all() == []
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_enable_falha_inesperada_vira_backenderror_e_offline() -> None:
    """2.4 — falha NÃO-BackendError no enable não vira 500 cru na rota.

    Um bug real no start (RuntimeError, não modelado como BackendError) tem o
    mesmo desfecho do caminho de erro conhecido: estado OFFLINE (volta ao
    ciclo do monitor) e BackendError propagado com a causa original no
    ``__cause__`` — a rota continua respondendo 503 estruturado.
    """
    manager, _ = make_fake_manager(("backend-a",))
    await manager.start_all()
    try:
        await manager.disable("backend-a")

        async def boom(name: str) -> None:
            raise RuntimeError("bug inesperado no start")

        manager._start_one = boom  # type: ignore[method-assign]
        with pytest.raises(BackendError, match="não subiu após enable"):
            await manager.enable("backend-a")
        assert manager.status_of("backend-a") is BackendStatus.OFFLINE
        assert manager.registries[0].list_all() == []
    finally:
        await manager.stop_all()


# ----------------------------------------------------------------------
# restart manual
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restart_manual_em_backend_running() -> None:
    """Restart proposital de um backend saudável (não precisa ter caído)."""
    manager, factory = make_fake_manager(("backend-a",))
    zero_backoff(manager)
    await manager.start_all()
    try:
        old_client = factory.created[0]
        await manager.restart("backend-a")
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        assert old_client.stopped
        assert len(factory.created) == 2
        assert [e.namespaced for e in manager.registries[0].list_all()] == ["backend-a.echo"]
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_restart_manual_recupera_backend_failed() -> None:
    """failed é terminal para o monitor, mas restart manual o recupera.

    Única saída de 'failed' sem reiniciar o Gateway inteiro.
    """
    manager, _ = make_fake_manager(("backend-a",), max_restart_attempts=2)
    zero_backoff(manager)
    await manager.start_all()
    try:
        state = manager.get_state("backend-a")
        state.status = BackendStatus.FAILED  # como se tivesse esgotado as tentativas
        state.client = None
        manager._unregister("backend-a")
        assert manager.registries[0].list_all() == []

        await manager.restart("backend-a")
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        assert [e.namespaced for e in manager.registries[0].list_all()] == ["backend-a.echo"]
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_restart_manual_falhando_propaga_erro() -> None:
    manager, _ = make_fake_manager(("backend-a",))
    await manager.start_all()
    try:
        manager._create_client = lambda backend_config: FakeClient(start_error=True)  # type: ignore[method-assign,return-value]
        with pytest.raises(BackendError, match="não subiu no restart manual"):
            await manager.restart("backend-a")
        assert manager.status_of("backend-a") is BackendStatus.OFFLINE
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_restart_manual_falha_inesperada_vira_backenderror_e_offline() -> None:
    """2.4 — falha NÃO-BackendError no restart não deixa o backend preso.

    Sem esta correção, um RuntimeError vindo de _start_one escaparia do
    `except BackendError` e o backend ficaria em RESTARTING para sempre.
    O desfecho correto: OFFLINE (com registries limpos) + BackendError.
    """
    manager, _ = make_fake_manager(("backend-a",))
    await manager.start_all()
    try:
        async def boom(name: str) -> None:
            raise RuntimeError("bug inesperado no start")

        manager._start_one = boom  # type: ignore[method-assign]
        with pytest.raises(BackendError, match="não subiu no restart manual"):
            await manager.restart("backend-a")
        assert manager.status_of("backend-a") is BackendStatus.OFFLINE
        assert manager.registries[0].list_all() == []
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_restart_manual_cancela_restart_pendente() -> None:
    """restart() assume o controle de um restart agendado (cancela a task)."""
    manager, factory = make_fake_manager(("backend-a",))
    manager._backoff_seconds = lambda failures: 5.0  # type: ignore[method-assign]
    await manager.start_all()
    try:
        state = manager.get_state("backend-a")
        await state.client.stop()
        await manager.check_and_recover("backend-a")  # agenda restart (backoff 5s)
        pending = manager._restart_tasks.get("backend-a")
        assert pending is not None and not pending.done()

        await manager.restart("backend-a")
        assert pending.cancelled()
        assert manager._restart_tasks.get("backend-a") is None
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        assert len(factory.created) == 2
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_restart_manual_nao_dispara_restart_concorrente_do_monitor() -> None:
    manager, factory = make_fake_manager(("backend-a",))
    await manager.start_all()
    entered = asyncio.Event()
    release = asyncio.Event()
    original_start_one = manager._start_one

    async def slow_start_one(name: str) -> None:
        entered.set()
        await release.wait()
        await original_start_one(name)

    manager._start_one = slow_start_one  # type: ignore[method-assign]
    restart_task = asyncio.create_task(manager.restart("backend-a"))
    try:
        await entered.wait()
        assert manager.status_of("backend-a") is BackendStatus.RESTARTING
        await manager.check_and_recover("backend-a")
        release.set()
        await restart_task
        await asyncio.sleep(0)
        assert len(factory.created) == 2
        assert all(client.stopped for client in factory.created[:1])
    finally:
        release.set()
        if not restart_task.done():
            await restart_task
        await manager.stop_all()


# ----------------------------------------------------------------------
# Observabilidade do estado disabled
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_summary_conta_disabled_sem_degradar() -> None:
    """Backend disabled NÃO degrada o /health — foi decisão do operador."""
    manager, _ = make_fake_manager(("backend-a", "backend-b"))
    zero_backoff(manager)
    await manager.start_all()
    try:
        await manager.disable("backend-a")
        summary = manager.health_summary()
        assert summary["status"] == "ok"  # disabled não é falha
        assert summary["backends"]["disabled"] == 1
        assert summary["backends"]["running"] == 1
        assert summary["backends"]["total"] == 2
        assert summary["tools_count"] == 1  # só as do backend-b

        details = {d["name"]: d for d in manager.server_details()}
        assert details["backend-a"]["status"] == "disabled"
        assert details["backend-a"]["tools_count"] == 0
        assert details["backend-b"]["status"] == "running"
    finally:
        await manager.stop_all()
