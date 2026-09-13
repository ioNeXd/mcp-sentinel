"""BackendManager: dono do ciclo de vida e do estado dos backends (Fase 2).

Única fonte de verdade sobre o estado de cada backend — ``main.py``, o Health
Monitor e as rotas de observabilidade consultam esta classe, nunca mantêm
estado próprio duplicado. Fluxo de atualização dos registries (o padrão de
snapshot imutável da Fase 1 é o que torna isso seguro): **antes** de qualquer
troca de processo os registros antigos do backend são removidos dos registries
e só depois os novos são registrados — os leitores (requests em andamento)
sempre enxergam um snapshot consistente, sem locks e sem itens órfãos/obsoletos.
"""

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

import structlog

from gateway.clients.base import BaseClient
from gateway.config import BackendConfig, BackendType, GatewayConfig
from gateway.errors import (
    BackendError,
    BackendJsonRpcError,
    BackendNotFoundError,
    BackendStateConflictError,
)
from gateway.registries import PromptRegistry, ResourceRegistry, ToolRegistry

logger = structlog.get_logger(__name__)

BASE_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 30.0
BACKOFF_MULTIPLIER = 2.0

HEALTH_PING_TIMEOUT_SECONDS = 2.0
"""Timeout do ping do health check.  
  
Um backend vivo responde "pong" bem abaixo do timeout de request normal —  
esperar o timeout longo apenas atrasaria a detecção de queda sem ganho algum.  
"""


class BackendStatus(str, Enum):
    """Estados possíveis de um backend gerenciado.

    ``DISABLED`` é o estado neutro de "desligado por pedido" (Fase 4): não é
    falha — o Health Monitor NUNCA tenta reiniciar um backend disabled (ver
    ``check_and_recover``); só ``enable()`` (ou restart do Gateway) o traz de
    volta. ``FAILED`` é terminal: o backend esgotou ``max_restart_attempts`` e
    exige intervenção humana.
    """

    RUNNING = "running"
    OFFLINE = "offline"
    RESTARTING = "restarting"
    FAILED = "failed"
    DISABLED = "disabled"


@dataclass
class BackendState:
    """Estado observável de um backend gerenciado pelo BackendManager.

    Attributes:
        name: Nome do backend no config (prefixo de namespace).
        config: Configuração (command/args) usada para (re)iniciar o processo.
        status: Estado atual no ciclo de vida.
        client: Client ativo, ou None quando não há processo vivo (offline/
            restarting/failed/disabled).
        consecutive_failures: Quedas em offline consecutivas; também determina
            o backoff do próximo restart.
        last_restart_at: Timestamp monotônico do último restart iniciado, para
            observabilidade (``/api/servers``).
        connected_since: Timestamp de relógio (``time.time()``) de quando o
            backend entrou em RUNNING pela última vez — usado pela página de
            detalhe do backend (Fase 7) para calcular uptime. ``None`` enquanto
            nunca conectou.
        warn_disabled_logged: Guarda do aviso periódico do monitor para backends
            disabled (evita logar o mesmo aviso a cada ciclo).
        terminal_logged: Guarda de ``backend_failed_terminal`` — sem isso, o
            Health Monitor loga o mesmo erro TODO ciclo enquanto o backend
            permanece FAILED (mesmo padrão de ``warn_disabled_logged``).
            Resetado a cada start bem-sucedido (``_start_one``).
    """

    name: str
    config: BackendConfig
    status: BackendStatus = BackendStatus.OFFLINE
    client: BaseClient | None = None
    consecutive_failures: int = 0
    last_restart_at: float | None = None
    connected_since: float | None = None
    warn_disabled_logged: bool = False
    terminal_logged: bool = False


class BackendManager:
    """Gerencia N backends: inicia, monitora estado, reinicia com backoff.

    Attributes:
        config: Configuração completa do Gateway.
        registries: Tupla de registries agregados (tools/resources/prompts) que
            este manager mantém sincronizados com os backends vivos.
    """

    def __init__(
        self,
        config: GatewayConfig,
        registries: tuple[ToolRegistry, ResourceRegistry, PromptRegistry],
    ) -> None:
        self.config = config
        self.registries = registries
        self._states: dict[str, BackendState] = {
            backend.name: BackendState(name=backend.name, config=backend)
            for backend in config.backends
        }
        self._restart_locks: dict[str, asyncio.Lock] = {
            name: asyncio.Lock() for name in self._states
        }
        self._restart_tasks: dict[str, asyncio.Task[None]] = {}

    # ------------------------------------------------------------------
    # Consulta de estado
    # ------------------------------------------------------------------

    def get_state(self, backend_name: str) -> BackendState | None:
        """Devolve o estado de um backend pelo nome, ou None se desconhecido."""
        return self._states.get(backend_name)

    def get_client(self, backend_name: str) -> BaseClient:
        """Devolve o client ativo do backend para roteamento (McpServer).

        Raises:
            BackendError: se o backend não existe no config ou não está
                disponível (running) no momento.
        """
        state = self._states.get(backend_name)
        if state is None:
            raise BackendNotFoundError(f"backend '{backend_name}' não existe no config")
        if state.status is not BackendStatus.RUNNING or state.client is None:
            raise BackendError(f"backend '{backend_name}' não está disponível")
        return state.client

    def status_of(self, backend_name: str) -> BackendStatus:
        """Devolve o status atual do backend (OFFLINE se desconhecido)."""
        state = self._states.get(backend_name)
        return state.status if state is not None else BackendStatus.OFFLINE

    def all_states(self) -> dict[str, BackendState]:
        """Devolve uma cópia do mapa de estados (uso em /health e /api/servers)."""
        return dict(self._states)

    def health_summary(self) -> dict[str, Any]:
        """Resumo geral do Gateway para GET /health.

        Backends disabled NÃO degradam o status geral: desligá-los foi uma
        decisão intencional do operador, não uma falha (Fase 4).
        """
        states = list(self._states.values())
        running = sum(1 for s in states if s.status is BackendStatus.RUNNING)
        failed = sum(1 for s in states if s.status is BackendStatus.FAILED)
        disabled = sum(1 for s in states if s.status is BackendStatus.DISABLED)
        return {
            "status": ("ok" if failed == 0 and (running + disabled) == len(states) else "degraded"),
            "backends": {
                "total": len(states),
                "running": running,
                "offline": sum(1 for s in states if s.status is BackendStatus.OFFLINE),
                "restarting": sum(1 for s in states if s.status is BackendStatus.RESTARTING),
                "failed": failed,
                "disabled": disabled,
            },
            "tools_count": len(self.registries[0].list_all()),
            "resources_count": len(self.registries[1].list_all()),
            "prompts_count": len(self.registries[2].list_all()),
        }

    def server_details(self) -> list[dict[str, Any]]:
        """Detalhe de cada backend para GET /api/servers."""
        tools, resources, prompts = self.registries
        details: list[dict[str, Any]] = []
        for state in self._states.values():
            tools_count = sum(1 for e in tools.list_all() if e.backend == state.name)
            resources_count = sum(1 for e in resources.list_all() if e.backend == state.name)
            prompts_count = sum(1 for e in prompts.list_all() if e.backend == state.name)
            backend_config: BackendConfig = state.config
            details.append(
                {
                    "name": state.name,
                    "status": state.status.value,
                    "type": backend_config.type.value,
                    "command": backend_config.command,
                    "args": list(backend_config.args),
                    "url": backend_config.url,
                    "consecutive_failures": state.consecutive_failures,
                    "last_restart_at": state.last_restart_at,
                    "connected_since": state.connected_since,
                    "tools_count": tools_count,
                    "resources_count": resources_count,
                    "prompts_count": prompts_count,
                    "transport": type(state.client).__name__ if state.client else None,
                }
            )
        return details

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def start_all(self) -> None:
        """Sobe todos os backends do config (handshake + registro nos registries).

        Cada backend é tratado isoladamente: um que falhe ao subir vira offline
        (o auto-restart cuida dele depois) — não derruba os demais nem o
        Gateway. Levanta BackendError somente se TODOS falharem.
        """
        failures = 0
        for name in self._states:
            async with self._restart_locks[name]:
                try:
                    await self._start_one(name)
                except BackendError as exc:
                    failures += 1
                    logger.error("backend_start_failed", backend=name, error=str(exc))
                    self._states[name].status = BackendStatus.OFFLINE
                    self._states[name].consecutive_failures = 1
        if failures == len(self._states):
            raise BackendError("nenhum backend conseguiu iniciar")

    async def add_backend(self, backend_config: BackendConfig) -> None:
        """Registra e sobe UM backend novo em tempo real (Fase 7).

        Usado pelo painel do dashboard (``POST /api/config/backends``). Não
        toca em nenhum backend existente: só adiciona entradas novas aos dicts
        internos (``_states``, ``_restart_locks``) e reaproveita o mesmo
        caminho de start do boot (``_start_one``), sob o lock próprio do
        backend novo — nunca precisa de lock global, então os outros backends
        seguem intocados durante o start.

        Uma falha ao subir (``BackendError``) NÃO propaga: o backend fica
        ``OFFLINE`` com ``consecutive_failures=1``, exatamente como
        ``start_all()`` trata um backend que falha no boot — o ``HealthMonitor``
        assume o restart (com backoff) a partir daqui, igual a qualquer outro
        backend offline.

        Raises:
            ValueError: já existe um backend com esse nome. É uma checagem
                redundante com a da rota HTTP (que já valida antes de chamar),
                mas o manager não confia cegamente no chamador — chamar isso
                fora da rota também fica seguro.
        """
        name = backend_config.name
        if name in self._states:
            raise ValueError(f"backend '{name}' já existe")
        state = BackendState(name=name, config=backend_config)
        self._states[name] = state
        self._restart_locks[name] = asyncio.Lock()
        async with self._restart_locks[name]:
            try:
                await self._start_one(name)
            except BackendError as exc:
                logger.error("backend_start_failed", backend=name, error=str(exc))
                state.status = BackendStatus.OFFLINE
                state.consecutive_failures = 1

    async def remove_backend(self, backend_name: str) -> None:
        """Remove um backend por completo, em tempo real (Fase 7).

        Usado pelo botão "remover" do card no dashboard (``DELETE
        /api/servers/{name}``). Diferente de ``disable()`` — que mantém o
        backend no config em estado neutro — isto tira o backend inteiramente
        do Gateway em memória: cancela restart pendente, para o client (se
        houver) e apaga a entrada de ``_states``/``_restart_locks``. A remoção
        do ``config.json`` em disco é responsabilidade do chamador HTTP (o
        manager não sabe onde o config mora).

        Idempotente do ponto de vista do manager não — chamar duas vezes
        levanta ``BackendNotFoundError`` na segunda, o que é o comportamento
        certo pra rota HTTP responder 404 em vez de 200 silencioso.

        Raises:
            BackendNotFoundError: backend não existe (já removido, ou nunca
                existiu neste processo).
        """
        state = self._states.get(backend_name)
        if state is None:
            raise BackendNotFoundError(f"backend '{backend_name}' não existe")
        pending = self._restart_tasks.get(backend_name)
        if pending is not None and not pending.done():
            pending.cancel()
            try:
                await pending
            except asyncio.CancelledError:
                pass
            self._restart_tasks.pop(backend_name, None)
        async with self._restart_locks[backend_name]:
            state = self._states.get(backend_name)
            if state is None:
                raise BackendNotFoundError(f"backend '{backend_name}' não existe")
            await self._teardown_client(state, next_status=BackendStatus.OFFLINE)
            del self._states[backend_name]
        del self._restart_locks[backend_name]
        logger.info("backend_removed", backend=backend_name)

    async def stop_all(self) -> None:
        """Cancela restarts pendentes, para os backends e limpa os registries.

        Idempotente. Uma falha ao parar um backend é capturada/logada pelo
        helper de teardown: o loop segue e TODO backend passa pelo cleanup
        completo, sem deixar processo órfão.
        """
        for task in self._restart_tasks.values():
            task.cancel()
        await asyncio.gather(*self._restart_tasks.values(), return_exceptions=True)
        self._restart_tasks.clear()
        for state in self._states.values():
            await self._teardown_client(state, next_status=BackendStatus.OFFLINE)
        logger.info("backend_manager_stopped", backends=len(self._states))

    # ------------------------------------------------------------------
    # Health check e restart
    # ------------------------------------------------------------------

    async def check_and_recover(self, backend_name: str) -> BackendStatus:
        """Serializa toda a decisão de health/recovery por backend."""
        state = self._states.get(backend_name)
        if state is None:
            return BackendStatus.OFFLINE
        async with self._restart_locks[backend_name]:
            return await self._check_and_recover_locked(backend_name, state)

    async def _check_and_recover_locked(
        self, backend_name: str, state: BackendState
    ) -> BackendStatus:
        """Um passo do Health Monitor para um backend: checa e tenta recuperar.

        Sequência por backend: processo morto/sem resposta → offline (registros
        removidos dos registries); offline + auto_restart → tentativa de restart
        (backoff exponencial, limite de tentativas); recuperado → running com
        registros novos. Falhas são tratadas localmente: nunca propagam para o
        monitor nem derrubam o Gateway.

        Backends DISABLED são intocáveis para o monitor (Fase 4): o desligamento
        foi intencional, então não há checagem de saúde nem restart — a única
        saída desse estado é ``enable()``. Um aviso é logado apenas UMA vez por
        período disabled (não a cada ciclo).
        """
        if state.status is BackendStatus.DISABLED:
            if not state.warn_disabled_logged:
                logger.info("backend_disabled_ignorado_pelo_monitor", backend=backend_name)
                state.warn_disabled_logged = True
            return BackendStatus.DISABLED

        was_running = state.status is BackendStatus.RUNNING
        if was_running and not await self._is_backend_alive(state):
            logger.warning(
                "backend_detected_offline",
                backend=backend_name,
                consecutive_failures=state.consecutive_failures + 1,
            )
            await self._teardown_client(state, next_status=BackendStatus.OFFLINE)
            state.consecutive_failures += 1
        elif was_running:
            if state.consecutive_failures > 0:
                logger.info("backend_healthy_again", backend=backend_name)
                state.consecutive_failures = 0

        if state.status is BackendStatus.OFFLINE:
            if self.config.auto_restart:
                self._schedule_restart(state)
            else:
                logger.warning("backend_offline_sem_auto_restart", backend=backend_name)
        elif state.status is BackendStatus.FAILED:
            if not state.terminal_logged:
                self._log_terminal(state)
                state.terminal_logged = True
        return state.status

    async def _is_backend_alive(self, state: BackendState) -> bool:
        """Saúde = transporte vivo E respondendo ``ping``.

        A checagem é idêntica para os três transportes (stdio/http/sse): o
        ``is_alive()`` pega o caso barato (processo morto no stdio, stream
        fechado no SSE, client fechado no HTTP) sem I/O; o ``ping`` com timeout
        curto pega o peer vivo mas travado. Uma resposta de erro JSON-RPC ao
        ping (backend que não implementa o método) ainda prova que o peer está
        vivo e falando o protocolo — só falhas de transporte (timeout, conexão
        fechada) contam como offline.
        """
        client = state.client
        if client is None or not client.is_alive():
            return False
        try:
            await asyncio.wait_for(
                client.send_request("ping", {}), timeout=HEALTH_PING_TIMEOUT_SECONDS
            )
        except BackendJsonRpcError:
            return True
        except (BackendError, asyncio.TimeoutError):
            # BackendError inclui BackendTimeoutError; TimeoutError puro cobre o
            # wait_for estourando antes de o client converter a falha.
            return False
        return True

    def _schedule_restart(self, state: BackendState) -> None:
        """Agenda a tentativa de restart em task própria (uma por backend).

        Fazê-lo em task separada garante que o backoff (até 30s) não bloqueia o
        ciclo do monitor nem a recuperação dos outros backends. Se já há um
        restart em andamento para este backend, não agenda outro.
        """
        pending = self._restart_tasks.get(state.name)
        if pending is not None and not pending.done():
            return
        self._restart_tasks[state.name] = asyncio.create_task(
            self._restart_task(state.name), name=f"restart-{state.name}"
        )

    async def _restart_task(self, name: str) -> None:
        """Wrapper que isola erros inesperados da task de restart.

        A política de falha de tentativa (contador, backoff, limite) vive toda
        dentro de ``_attempt_restart``; este wrapper é só a rede de segurança
        para bugs fora desse escopo. Uma falha inesperada aqui também conta para
        a política de backoff/limite (mesmo efeito do caminho de BackendError):
        o backend vira OFFLINE e o contador é incrementado.

        No ``finally`` a própria referência de task é removida — mas só se ainda
        for a task atual daquele backend (uma task mais nova que já a substituiu
        não é tocada).
        """
        try:
            await self._attempt_restart(self._states[name])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("erro inesperado no restart do backend", backend=name)
            state = self._states[name]
            state.status = BackendStatus.OFFLINE
            state.consecutive_failures += 1
        finally:
            task = asyncio.current_task()
            if task is not None and self._restart_tasks.get(name) is task:
                self._restart_tasks.pop(name, None)

    async def _attempt_restart(self, state: BackendState) -> None:
        """Tenta reiniciar um backend offline, respeitando backoff e limite.

        Semântica de ``max_restart_attempts`` (documentada no README): é o
        NÚMERO DE TENTATIVAS de restart, não tentativas - 1. O corte usa ``>`` e
        não ``>=``: com ``consecutive_failures == N`` ainda há a N-ésima
        tentativa a fazer; só quando as falhas acumuladas (queda inicial +
        restarts falhos) EXCEDEM N o backend vira FAILED — ou seja, exatamente
        N tentativas de restart acontecem antes do estado terminal.

        Qualquer falha na tentativa (BackendError ou não) passa pela mesma
        política: OFFLINE + incremento do contador, para que backoff e limite
        continuem valendo também para falhas inesperadas.
        """
        async with self._restart_locks[state.name]:
            state = self._states[state.name]
            if state.status is not BackendStatus.OFFLINE:
                return  # outro caminho já recuperou (ou falhou) este backend
            if state.consecutive_failures > self.config.max_restart_attempts:
                state.status = BackendStatus.FAILED
                if not state.terminal_logged:
                    self._log_terminal(state)
                    state.terminal_logged = True
                return
            delay = self._backoff_seconds(state.consecutive_failures)
            logger.info(
                "backend_restart_scheduled",
                backend=state.name,
                delay_seconds=delay,
                attempt=state.consecutive_failures,
            )
            await asyncio.sleep(delay)
            state.status = BackendStatus.RESTARTING
            state.last_restart_at = time.monotonic()
            try:
                await self._start_one(state.name)
            except Exception as exc:
                state.status = BackendStatus.OFFLINE
                state.consecutive_failures += 1
                if isinstance(exc, BackendError):
                    logger.warning(
                        "backend_restart_failed",
                        backend=state.name,
                        attempt=state.consecutive_failures,
                        error=str(exc),
                    )
                else:
                    logger.exception(
                        "backend_restart_failed",
                        backend=state.name,
                        attempt=state.consecutive_failures,
                        error=str(exc),
                    )
            else:
                logger.info("backend_recovered", backend=state.name)

    async def disable(self, backend_name: str) -> None:
        """Desliga um backend INTENCIONALMENTE (rota /disable, Fase 4).

        Diferente de uma queda: para o client (se houver), remove os registros
        dos registries, cancela qualquer restart agendado e entra no estado
        DISABLED — que o Health Monitor NÃO tenta recuperar (ver
        ``check_and_recover``). Idempotente: desabilitar um backend já disabled
        não faz nada.

        Usa o mesmo lock de ``restart()``/``enable()``: um restart manual em
        andamento (dentro do lock, em ``_start_one``) não pode sobrescrever
        este disable ao terminar — o disable espera a seção crítica concluir e
        o estado final é sempre DISABLED, nunca RUNNING.

        Raises:
            BackendError: se o backend não existe no config.
        """
        state = self._states.get(backend_name)
        if state is None:
            raise BackendNotFoundError(f"backend '{backend_name}' não existe no config")
        if state.status is BackendStatus.DISABLED:
            return
        pending = self._restart_tasks.get(backend_name)
        if pending is not None and not pending.done():
            pending.cancel()
            try:
                await pending
            except asyncio.CancelledError:
                pass
            self._restart_tasks.pop(backend_name, None)
        async with self._restart_locks[backend_name]:
            await self._teardown_client(state, next_status=BackendStatus.DISABLED)
            state.warn_disabled_logged = False
        logger.info("backend_disabled", backend=backend_name)

    async def enable(self, backend_name: str) -> None:
        """Reverte um backend DISABLED (rota /enable, Fase 4).

        Mesmo fluxo de um restart bem-sucedido: sobe o client do tipo certo,
        refaz o handshake e reintegra tools/resources/prompts nos registries.
        Se a subida falhar, o backend fica offline e volta para o ciclo normal
        do Health Monitor (auto-restart) — mas o erro é PROPAGADO para a rota
        responder 503 (diferente do restart automático, que é fire-and-forget).

        Usa o mesmo lock de ``restart()``/``disable()``/``_attempt_restart()``:
        sem ele, um enable() e um restart() manual (ou uma tentativa automática)
        quase simultâneos poderiam chamar ``_start_one()`` ao mesmo tempo para o
        mesmo backend, registrando dois clients. A revalidação do status DEPOIS
        de pegar o lock cobre o caso em que o backend deixou de estar DISABLED
        enquanto esperávamos a seção crítica (ex.: outro enable()/restart() já
        concluiu primeiro).

        Raises:
            BackendError: backend inexistente, ou não disabled (use /restart),
                ou a subida falhou (detalhe no ``__cause__``).
        """
        state = self._states.get(backend_name)
        if state is None:
            raise BackendNotFoundError(f"backend '{backend_name}' não existe no config")
        if state.status is not BackendStatus.DISABLED:
            raise BackendStateConflictError(
                f"backend '{backend_name}': enable só se aplica a backends disabled"
                f" (status atual: {state.status.value})"
            )
        async with self._restart_locks[backend_name]:
            state = self._states[backend_name]
            if state.status is not BackendStatus.DISABLED:
                raise BackendStateConflictError(
                    f"backend '{backend_name}': enable só se aplica a backends disabled"
                    f" (status atual: {state.status.value})"
                )
            await self._start_one_locked(backend_name, state)
        logger.info("backend_enabled", backend=backend_name)

    async def _start_one_locked(self, backend_name: str, state: BackendState) -> None:
        """Corpo de enable() que precisa rodar dentro do restart lock.

        Uma falha inesperada (não-BackendError) deixa o estado consistente
        (OFFLINE, volta ao ciclo normal do monitor) e é re-levantada como
        BackendError para a rota responder 503 estruturado em vez de 500 cru,
        preservando a causa original em ``__cause__``.
        """
        try:
            await self._start_one(backend_name)
        except BackendError as exc:
            state.status = BackendStatus.OFFLINE
            state.consecutive_failures = 1
            logger.warning("backend_enable_failed", backend=backend_name, error=str(exc))
            raise BackendError(f"backend '{backend_name}' não subiu após enable: {exc}") from exc
        except Exception as exc:
            state.status = BackendStatus.OFFLINE
            state.consecutive_failures = 1
            logger.exception("backend_enable_failed", backend=backend_name, error=str(exc))
            raise BackendError(f"backend '{backend_name}' não subiu após enable: {exc}") from exc

    async def restart(self, backend_name: str) -> None:
        """Restart manual imediato — funciona em QUALQUER estado (Fase 4).

        Uso: rota ``/api/servers/{name}/restart``, inclusive para um backend
        ``running`` (restart proposital de manutenção) ou ``failed`` (única
        forma de recuperar sem reiniciar o Gateway inteiro). Cancela restart
        agendado, derruba o client atual e sobe um novo.

        Diferente do restart automático (fire-and-forget), aqui a falha é
        PROPAGADA para a rota responder 503 — o chamador espera o resultado.
        Uma falha inesperada não deixa o backend preso em RESTARTING nem vira
        500 cru: mesmo desfecho do caminho de BackendError, com a causa
        original preservada.

        Raises:
            BackendError: backend inexistente, ou a subida falhou (detalhe no
                ``__cause__``).
        """
        state = self._states.get(backend_name)
        if state is None:
            raise BackendNotFoundError(f"backend '{backend_name}' não existe no config")
        pending = self._restart_tasks.get(backend_name)
        if pending is not None and not pending.done():
            pending.cancel()  # o restart manual assume o controle
            try:
                await pending
            except asyncio.CancelledError:
                pass
            self._restart_tasks.pop(backend_name, None)
        async with self._restart_locks[backend_name]:
            state = self._states[backend_name]
            await self._teardown_client(state, next_status=BackendStatus.RESTARTING)
            state.consecutive_failures = 0
            state.warn_disabled_logged = False
            try:
                await self._start_one(backend_name)
            except BackendError as exc:
                state.status = BackendStatus.OFFLINE
                state.consecutive_failures = 1
                logger.warning(
                    "backend_manual_restart_failed",
                    backend=backend_name,
                    error=str(exc),
                )
                raise BackendError(
                    f"backend '{backend_name}' não subiu no restart manual: {exc}"
                ) from exc
            except Exception as exc:
                state.status = BackendStatus.OFFLINE
                state.consecutive_failures = 1
                logger.exception(
                    "backend_manual_restart_failed",
                    backend=backend_name,
                    error=str(exc),
                )
                raise BackendError(
                    f"backend '{backend_name}' não subiu no restart manual: {exc}"
                ) from exc
        logger.info("backend_restarted_manualmente", backend=backend_name)

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    async def _teardown_client(self, state: BackendState, *, next_status: BackendStatus) -> None:
        """Para o client, limpa registros e aplica a transição de status.

        Ordem fixa e resiliente: parar o client (falha aqui é capturada e
        logada — nunca impede os passos seguintes), zerar a referência, remover
        o backend dos registries e só então aplicar o novo status. Usado por
        stop_all/disable/restart e pela detecção de offline — os quatro pontos
        que repetiam a mesma sequência, cada um com seu próprio ponto frágil
        (um stop() que estourasse deixava o estado interno inconsistente com o
        registry e com a referência ao client).
        """
        client = state.client
        if client is not None:
            try:
                await client.stop()
            except Exception:
                logger.exception("backend_stop_failed", backend=state.name)
        state.client = None
        self._unregister(state.name)
        state.status = next_status

    async def _start_one(self, name: str) -> None:
        """Sobe o client do backend, faz handshake e registra nos registries.

        O registro é atômico: se qualquer ``register`` falhar (ex.: item
        duplicado/inválido vindo do backend), desfaz os já feitos nesta
        tentativa e para o client — nenhum registro órfão, nenhuma
        conexão/processo órfão, e o estado fica consistente para o chamador
        tratar como falha de start.
        """
        state = self._states[name]
        client = self._create_client(state.config)
        try:
            await client.start()
            tools = await client.list_tools()
            resources = await client.list_resources()
            prompts = await client.list_prompts()
            self.registries[0].register(name, tools)
            self.registries[1].register(name, resources)
            self.registries[2].register(name, prompts)
        except BaseException:
            self._unregister(name)
            try:
                await client.stop()
            except Exception:
                logger.exception("backend_stop_failed", backend=name)
            raise
        state.client = client
        state.status = BackendStatus.RUNNING
        state.consecutive_failures = 0
        state.warn_disabled_logged = False
        state.terminal_logged = False
        state.connected_since = time.time()
        logger.info(
            "backend_connected",
            backend=name,
            tools=len(tools),
            resources=len(resources),
            prompts=len(prompts),
        )

    def _create_client(self, backend_config: Any) -> BaseClient:
        """Fábrica de clients: escolhe o transporte pelo ``type`` do config (Fase 3).

        A construção é síncrona (sem I/O); a conexão de fato acontece no
        ``await client.start()`` feito pelo chamador. Tipos desconhecidos não
        chegam aqui: o ``BackendType`` do Pydantic rejeita valores inválidos
        já no load do config.
        """
        assert isinstance(backend_config, BackendConfig)  # noqa: S101 — guarda de contrato
        timeout = self.config.request_timeout_for(backend_config)
        match backend_config.type:
            case BackendType.STDIO:
                from gateway.clients.stdio_client import StdioClient

                return StdioClient(backend_config, request_timeout=timeout)
            case BackendType.HTTP:
                from gateway.clients.http_client import HttpClient

                return HttpClient(backend_config, request_timeout=timeout)
            case BackendType.SSE:
                from gateway.clients.sse_client import SseClient

                return SseClient(backend_config, request_timeout=timeout)

    def _backoff_seconds(self, consecutive_failures: int) -> float:
        """Backoff exponencial 1s → 2s → 4s → 8s → 16s com cap em 30s."""
        failures = max(consecutive_failures, 1)
        delay = BASE_BACKOFF_SECONDS * BACKOFF_MULTIPLIER ** (failures - 1)
        return min(delay, MAX_BACKOFF_SECONDS)

    def _unregister(self, backend_name: str) -> None:
        for registry in self.registries:
            registry.unregister(backend_name)

    def _log_terminal(self, state: BackendState) -> None:
        logger.error(
            "backend_failed_terminal",
            backend=state.name,
            attempts=state.consecutive_failures,
            detail=(
                f"esgotou {self.config.max_restart_attempts} tentativas de restart"
                " — intervenção humana necessária (reinicie o Gateway)"
            ),
        )
