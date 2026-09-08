"""HealthMonitor: task assíncrona que verifica a saúde dos backends (Fase 2).

Loop leve: a cada ciclo chama ``BackendManager.check_and_recover(backend)`` para
cada backend gerenciado — a lógica de estado (offline/restart/backoff) vive toda
no manager; o monitor só orquestra o quando e isola falhas do próprio loop
(um erro inesperado num backend nunca derruba o monitor nem o Gateway).
"""

import asyncio

import structlog

from gateway.backend_manager import BackendManager

logger = structlog.get_logger(__name__)


class HealthMonitor:
    """Verifica os backends a cada ``interval_seconds`` e aciona a recuperação.

    Attributes:
        manager: BackendManager consultado a cada ciclo.
        interval_seconds: Intervalo entre ciclos (config ``health_check_interval_seconds``).
    """

    def __init__(self, manager: BackendManager, interval_seconds: float) -> None:
        self.manager = manager
        self.interval_seconds = interval_seconds
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Inicia o loop em background (idempotente)."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(
                self._run(), name="health-monitor"
            )

    async def stop(self) -> None:
        """Cancela o loop e aguarda a task terminar (idempotente).

        O ``CancelledError`` da task cancelada é o DESFECHO ESPERADO do
        cancelamento — se vies com ``return_exceptions=True`` é descartado
        aqui; se vies levantado, é capturado e suprimido. Qualquer OUTRA
        exceção é um erro real do loop e é re-levantada. Se o PRÓPRIO
        ``stop()`` for cancelado (ex.: shutdown global derrubando a task que
        o chama), o cancelamento do chamador propaga após o cleanup — nunca
        é engolido, para não mascarar um desligamento em andamento.
        """
        task = self._task
        if task is not None:
            self._task = None  # idempotente mesmo se algo falhar abaixo
            task.cancel()
            results = await asyncio.gather(task, return_exceptions=True)
            result = results[0] if results else None
            if isinstance(result, asyncio.CancelledError):
                pass  # cancelamento bem-sucedido: desfecho esperado, não é erro
            elif isinstance(result, BaseException):
                raise result
        logger.info("health_monitor_stopped")

    async def _run(self) -> None:
        """Loop principal: checa todos os backends, dorme o intervalo, repete.

        ``CancelledError`` propaga (é o mecanismo de parada do ``stop()``);
        qualquer outro erro do ciclo é logado e o loop continua.
        """
        logger.info("health_monitor_started", interval_seconds=self.interval_seconds)
        while True:
            try:
                await asyncio.sleep(self.interval_seconds)
                await self.check_all()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Um erro inesperado fora de um backend específico não deve
                # matar o monitor silenciosamente: loga e continua o loop.
                logger.exception("health_monitor_loop_error")

    async def check_all(self) -> None:
        """Um ciclo completo: checa e tenta recuperar cada backend, isoladamente."""
        for name in self.manager.all_states():
            try:
                await self.manager.check_and_recover(name)
            except Exception as exc:  # noqa: BLE001 — isolamento por backend
                logger.exception(
                    "health_check_unexpected_error", backend=name, error=str(exc)
                )
