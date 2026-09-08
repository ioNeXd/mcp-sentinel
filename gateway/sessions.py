"""SessionFilter: views seletivas de backends por sessão (Fase 5 do ROADMAP).

Motivação (do prompt da fase): o Gateway é consumido por um agente (OpenClaude)
com modelos gratuitos de contexto limitado e tool-calling menos confiável.
Expor todas as tools de todos os backends de uma vez pode estourar contexto ou
confundir o modelo — o filtro deixa a sessão ativar só o subconjunto de
backends de que precisa.

Princípios:
- **NÃO duplica dados**: os registries globais continuam sendo a fonte única de
  verdade com TUDO; aqui guarda-se apenas o subconjunto de nomes de backend que
  cada sessão ativou. A filtragem é calculada na hora de responder.
- **Compatibilidade**: sessão sem filtro vê tudo, exatamente como antes —
  clientes que não conhecem a extensão (Claude Desktop, ``mcp-remote``) não
  mudam de comportamento.
- **Isolamento**: cada sessão tem seu próprio conjunto; sessões com filtros
  diferentes nunca interferem uma na outra.

Identificação da sessão: header HTTP ``Mcp-Session-Id`` (nome do header
alinhado ao streamable HTTP do MCP, reutilizado aqui como mecanismo simples e
stateless-friendly). Sem o header, não há sessão — e sem sessão não há filtro
(comportamento default, total).
"""

import time
from typing import Callable

import structlog

logger = structlog.get_logger(__name__)

# Limpeza oportunista: a cada N operações de escrita, remove sessões expiradas.
PURGE_EVERY_N_WRITES = 32
# Independente da opórtuna, remove expiradas se passou este intervalo.
PURGE_INTERVAL_SECONDS = 300.0

SESSION_HEADER = "Mcp-Session-Id"


class SessionFilter:
    """Filtro de backends ativos por sessão, com expiração por inatividade.

    Attributes:
        ttl_seconds: Tempo de vida de uma sessão sem atividade. A cada acesso,
            o TTL é renovado — sessão ativa nunca expira no meio do uso.
        clock: Fonte de tempo monotônico (injetável para testes com mock de
            tempo; default ``time.monotonic``).
    """

    def __init__(
        self,
        ttl_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ttl_seconds = ttl_seconds
        self._clock = clock
        # session_id -> (conjunto de backends ativos, deadline de expiração).
        self._sessions: dict[str, tuple[frozenset[str], float]] = {}
        self._writes_since_purge = 0
        self._last_purge = clock()

    # ------------------------------------------------------------------
    # Escrita
    # ------------------------------------------------------------------

    def set_active_backends(self, session_id: str, backends: frozenset[str]) -> None:
        """Define (ou substitui) o filtro da sessão e renova o TTL.

        A validação de nomes (backends existentes, não vazios) é responsabilidade
        do chamador (McpServer) — aqui é armazenamento puro.
        """
        now = self._clock()
        self._sessions[session_id] = (frozenset(backends), now + self.ttl_seconds)
        self._maybe_purge(now)
        logger.info(
            "session_filter_set",
            session_id=session_id,
            backends=sorted(backends),
            ttl_seconds=self.ttl_seconds,
        )

    def clear(self, session_id: str) -> None:
        """Remove o filtro da sessão (volta a ver todos os backends)."""
        self._sessions.pop(session_id, None)
        logger.info("session_filter_cleared", session_id=session_id)

    # ------------------------------------------------------------------
    # Leitura
    # ------------------------------------------------------------------

    def active_backends(self, session_id: str | None) -> frozenset[str] | None:
        """Conjunto de backends da sessão, ou None se não há filtro.

        Acesso RENOVÁ o TTL (sessão em uso não expira). Sessão expirada é
        removida no próprio acesso — da perspectiva do chamador, deixa de
        existir e a sessão volta a ver tudo (comportamento seguro: o default
        é sempre ver tudo).
        """
        if session_id is None:
            return None
        entry = self._sessions.get(session_id)
        if entry is None:
            return None
        backends, deadline = entry
        now = self._clock()
        if now >= deadline:
            del self._sessions[session_id]
            logger.info("session_expired", session_id=session_id, ttl_seconds=self.ttl_seconds)
            return None
        self._sessions[session_id] = (backends, now + self.ttl_seconds)
        return backends

    # ------------------------------------------------------------------
    # Manutenção
    # ------------------------------------------------------------------

    def purge_expired(self) -> int:
        """Remove todas as sessões expiradas; devolve quantas foram removidas."""
        now = self._clock()
        expired = [sid for sid, (_, deadline) in self._sessions.items() if now >= deadline]
        for sid in expired:
            del self._sessions[sid]
        if expired:
            logger.info("sessions_purged", count=len(expired))
        self._last_purge = now
        self._writes_since_purge = 0
        return len(expired)

    def session_count(self) -> int:
        """Número de sessões com filtro armazenadas (uso em diagnóstico)."""
        return len(self._sessions)

    def _maybe_purge(self, now: float) -> None:
        """Limpeza oportunista: por contagem de escritas ou por intervalo."""
        self._writes_since_purge += 1
        if (
            self._writes_since_purge >= PURGE_EVERY_N_WRITES
            or (now - self._last_purge) >= PURGE_INTERVAL_SECONDS
        ):
            self.purge_expired()
