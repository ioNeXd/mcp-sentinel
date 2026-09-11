"""HealthMonitor: task assíncrona que verifica a saúde dos backends (Fase 2).

Loop leve: a cada ciclo dispara ``BackendManager.check_and_recover(backend)``
para cada backend gerenciado em PARALELO (com limite de concorrência). Toda a
lógica de estado (offline/restart/backoff) vive no manager; o monitor apenas
orquestra o *quando* e isola falhas — um erro inesperado num backend nunca
derruba o monitor nem o Gateway, nem atrasa a detecção dos demais backends no
mesmo ciclo.
"""

import asyncio

import structlog

from gateway.backend_manager import BackendManager

logger = structlog.get_logger(__name__)

# Teto de checks simultâneos por ciclo. O ping de cada backend tem timeout
# próprio, então o pior caso de um ciclo é ``teto * HEALTH_PING_TIMEOUT_SECONDS``.
# Valor pequeno basta: uso pessoal tem poucos backends e a checagem é I/O leve.
MAX_CONCURRENT_CHECKS = 8


class HealthMonitor:
    """Verifica os backends a cada ``interval_seconds`` e aciona a recuperação.

    Attributes:
        manager: BackendManager consultado a cada ciclo.
        interval_seconds: Intervalo entre ciclos (config
            ``health_check_interval_seconds``).
    """

    def __init__(self, manager: BackendManager, interval_seconds: float) -> None:
        self.manager = manager
        self.interval_seconds = interval_seconds
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Inicia o loop em background (idempotente)."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="health-monitor")

    async def stop(self) -> None:
        """Cancela o loop e aguarda a task terminar (idempotente).

        O ``CancelledError`` da task cancelada é o desfecho esperado do
        cancelamento e é descartado aqui. Qualquer OUTRA exceção é um erro real
        do loop e é re-levantada. Se o PRÓPRIO ``stop()`` for cancelado (ex.:
        shutdown global derrubando a task que o chama), o cancelamento do
        chamador propaga após o cleanup — nunca é engolido, para não mascarar
        um desligamento em andamento.
        """
        task = self._task
        if task is not None:
            self._task = None
            task.cancel()
            results = await asyncio.gather(task, return_exceptions=True)
            result = results[0] if results else None
            if isinstance(result, asyncio.CancelledError):
                pass
            elif isinstance(result, BaseException):
                raise result
        logger.info("health_monitor_stopped")

    async def _run(self) -> None:
        """Loop principal: checa todos os backends, dorme o intervalo, repete.

        O primeiro ciclo roda imediatamente ao entrar no loop (check antes do
        sleep): um backend que nasceu morto é detectado/reiniciado no primeiro
        instante, não depois de um intervalo inteiro. ``CancelledError`` propaga
        (é o mecanismo de parada do ``stop()``); qualquer outro erro do ciclo é
        logado e o loop continua, para não matar o monitor silenciosamente.
        """
        logger.info("health_monitor_started", interval_seconds=self.interval_seconds)
        while True:
            try:
                await self.check_all()
                await asyncio.sleep(self.interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("health_monitor_loop_error")

    async def check_all(self) -> None:
        """Um ciclo completo: checa e tenta recuperar cada backend em paralelo.

        Checks concorrentes com teto (``MAX_CONCURRENT_CHECKS``): um backend
        lento (ping pendendo até o timeout) não atrasa a detecção dos demais no
        mesmo ciclo. O isolamento de erro é POR backend — uma falha num
        ``check_and_recover`` é logada com o nome dele e não interrompe os
        outros (``gather(return_exceptions=True)`` devolve as exceções como
        resultados). O ``CancelledError`` do cancelamento externo (stop do
        monitor) propaga pelo gather como cancelamento do ciclo, sem virar
        "erro inesperado" de nenhum backend no log.
        """
        names = list(self.manager.all_states())
        if not names:
            return
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)

        async def check_one(name: str) -> None:
            async with semaphore:
                await self.manager.check_and_recover(name)

        results = await asyncio.gather(*(check_one(name) for name in names), return_exceptions=True)
        for name, result in zip(names, results):
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                logger.error(
                    "health_check_unexpected_error",
                    backend=name,
                    error=str(result),
                    exc_info=result,
                )
