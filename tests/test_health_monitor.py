"""Testes do HealthMonitor (Fase 2): loop periódico e isolamento de falhas.

Usa ciclos manuais (``check_all``) e intervalos curtos com sleeps reais mínimos;
o caminho de restart/backoff em si é coberto em test_backend_manager.py.
"""

import asyncio
from typing import Any

import pytest

from conftest import make_fake_manager
from gateway.backend_manager import BackendStatus
from gateway.health_monitor import HealthMonitor


@pytest.mark.asyncio
async def test_check_all_detecta_backend_morto_no_ciclo() -> None:
    manager, factory = make_fake_manager(("backend-a", "backend-b"))
    monitor = HealthMonitor(manager, interval_seconds=0.05)
    await manager.start_all()
    try:
        await factory.created[0].stop()  # backend-a morre
        await monitor.check_all()
        assert manager.status_of("backend-a") is BackendStatus.OFFLINE
        assert manager.status_of("backend-b") is BackendStatus.RUNNING
    finally:
        await monitor.stop()
        await manager.stop_all()


@pytest.mark.asyncio
async def test_loop_periodico_rodando_em_background(monkeypatch: pytest.MonkeyPatch) -> None:
    """O loop roda em background e executa ciclos no intervalo configurado."""
    manager, factory = make_fake_manager(("backend-a",))
    manager._backoff_seconds = lambda failures: 0.0  # type: ignore[method-assign]
    monitor = HealthMonitor(manager, interval_seconds=0.05)
    cycles: list[int] = []

    async def counted_check_all() -> None:
        cycles.append(1)
        await HealthMonitor.check_all(monitor)

    monkeypatch.setattr(monitor, "check_all", counted_check_all)
    await manager.start_all()
    try:
        monitor.start()
        await asyncio.sleep(0.18)  # ~3 ciclos a 0.05s
        assert len(cycles) >= 2
    finally:
        await monitor.stop()
        await manager.stop_all()


@pytest.mark.asyncio
async def test_erro_inesperado_num_backend_nao_derruba_o_monitor() -> None:
    manager, _ = make_fake_manager(("backend-a", "backend-b"))
    monitor = HealthMonitor(manager, interval_seconds=3600.0)
    await manager.start_all()
    try:
        # check_and_recover do backend-a explode; o ciclo continua no backend-b.
        async def exploding(name: str) -> BackendStatus:
            if name == "backend-a":
                raise RuntimeError("boom")
            return manager.status_of(name)

        manager.check_and_recover = exploding  # type: ignore[method-assign]
        await monitor.check_all()  # não levanta
        assert manager.status_of("backend-b") is BackendStatus.RUNNING
    finally:
        await monitor.stop()
        await manager.stop_all()


@pytest.mark.asyncio
async def test_stop_cancela_a_task_do_loop() -> None:
    manager, _ = make_fake_manager(("backend-a",))
    monitor = HealthMonitor(manager, interval_seconds=0.05)
    monitor.start()
    task = monitor._task
    assert task is not None and not task.done()
    await monitor.stop()
    assert task.done()
    await monitor.stop()  # idempotente


@pytest.mark.asyncio
async def test_stop_nao_propaga_cancellederror_da_task() -> None:
    """Regressão Fase 2: o CancelledError da task cancelada é o DESFECHO esperado.

    ``stop()`` deve retornar normalmente (nada de traceback de cancelamento no
    Ctrl+C) e a task deve terminar exatamente no estado "cancelada" — não com
    uma exceção não tratada.
    """
    manager, _ = make_fake_manager(("backend-a",))
    monitor = HealthMonitor(manager, interval_seconds=0.05)
    monitor.start()
    task = monitor._task
    assert task is not None
    await monitor.stop()  # não deve levantar nada
    assert task.done()
    assert task.cancelled()  # terminou por cancelamento, sem erro


@pytest.mark.asyncio
async def test_stop_re_levanta_erro_real_do_loop() -> None:
    """Erro genuíno do loop (não cancelamento) não pode ser engolido pelo stop()."""
    manager, _ = make_fake_manager(("backend-a",))
    monitor = HealthMonitor(manager, interval_seconds=0.05)

    async def broken() -> None:
        raise RuntimeError("erro real do loop")

    task = asyncio.create_task(broken(), name="health-monitor-broken")
    monitor._task = task
    # Deixa a task TERMINAR com o erro (create_task só agenda; se cancelada
    # antes de rodar, ela terminaria cancelada — outro teste já cobre isso).
    await asyncio.gather(task, return_exceptions=True)
    with pytest.raises(RuntimeError, match="erro real do loop"):
        await monitor.stop()
    assert monitor._task is None


@pytest.mark.asyncio
async def test_stop_propaga_cancelamento_do_proprio_chamador(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Se o PRÓPRIO stop() for cancelado (shutdown global derrubando a task
    que o chama), o cancelamento do chamador propaga após o cleanup — nunca é
    engolido, para não mascarar um desligamento em andamento.

    Determinismo: o ``asyncio.gather`` é substituído por um que espera numa
    barreira (``gate``), garantindo que o cancelamento caia DENTRO do stop(),
    depois de ele já ter cancelado a task do monitor.
    """
    manager, _ = make_fake_manager(("backend-a",))
    monitor = HealthMonitor(manager, interval_seconds=3600.0)
    monitor.start()
    inner = monitor._task
    assert inner is not None

    gate = asyncio.Event()
    real_gather = asyncio.gather

    async def gather_preso_na_barreira(*args: Any, **kwargs: Any) -> Any:
        await gate.wait()  # mantém stop() suspenso num ponto cancelável
        return await real_gather(*args, **kwargs)

    monkeypatch.setattr(asyncio, "gather", gather_preso_na_barreira)

    stop_task = asyncio.create_task(monitor.stop())
    await asyncio.sleep(0.05)  # stop() já cancelou a task e está preso na barreira
    stop_task.cancel()
    while not stop_task.done():
        await asyncio.sleep(0)
    monkeypatch.undo()  # restaura gather antes de qualquer outro uso de asyncio
    with pytest.raises(asyncio.CancelledError):
        await stop_task
    assert monitor._task is None  # o estado interno foi limpo mesmo propagando
    assert inner.done() and inner.cancelled()


@pytest.mark.asyncio
async def test_restart_de_backend_morto_durante_os_ciclos_do_monitor() -> None:
    """Cenário e2e da Fase 2: backend morre, monitor detecta e recupera."""
    manager, factory = make_fake_manager(("backend-a",))
    manager._backoff_seconds = lambda failures: 0.0  # type: ignore[method-assign]
    monitor = HealthMonitor(manager, interval_seconds=0.05)
    await manager.start_all()
    try:
        monitor.start()
        await factory.created[0].stop()  # processo morre
        # O monitor precisa detectar e reiniciar sem intervenção.
        for _ in range(20):
            if manager.status_of("backend-a") is BackendStatus.RUNNING and len(factory.created) >= 2:
                break
            await asyncio.sleep(0.03)
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        assert len(factory.created) >= 2  # client antigo + reiniciado
    finally:
        await monitor.stop()
        await manager.stop_all()
