"""Testes de evição de sessão no SessionFilter (cobertura de _enforce_max_sessions).

Antes desta suíte, _enforce_max_sessions nunca era exercitado diretamente —
os testes existentes cobriam TTL/purge/max_sessions indiretamente, mas não
validavam a lógica de qual sessão é removida quando o teto é atingido.
"""


from gateway.sessions import SessionFilter


class FakeClock:
    """Relógio controlável para testes de TTL/eviction."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_filter(max_sessions: int = 3, ttl: float = 60.0) -> SessionFilter:
    clock = FakeClock()
    return SessionFilter(
        clock=clock,
        ttl_seconds=ttl,
        max_sessions=max_sessions,
    )


class TestEnforceMaxSessions:
    """Testes de evição quando SessionFilter atinge o teto de sessões."""

    def test_sem_eviction_abaixo_do_limite(self) -> None:
        """Sessões abaixo do teto não são removidas."""
        sf = make_filter(max_sessions=5)
        sf.set_active_backends("s1", frozenset({"a"}))
        sf.set_active_backends("s2", frozenset({"b"}))
        sf.set_active_backends("s3", frozenset({"c"}))
        assert sf.session_count() == 3

    def test_eviction_remove_mais_antiga(self) -> None:
        """Sessão com deadline mais antigo é removida primeiro."""
        sf = make_filter(max_sessions=3, ttl=60.0)
        clock = sf._clock

        sf.set_active_backends("s1", frozenset({"a"}))
        clock.advance(10)
        sf.set_active_backends("s2", frozenset({"b"}))
        clock.advance(10)
        sf.set_active_backends("s3", frozenset({"c"}))
        # 3 sessões, teto = 3. Próxima write causa eviction.
        clock.advance(10)
        sf.set_active_backends("s4", frozenset({"d"}))

        assert sf.session_count() == 3
        assert sf.active_backends("s1") is None  # evicted (deadline mais antigo)
        assert sf.active_backends("s2") is not None
        assert sf.active_backends("s3") is not None
        assert sf.active_backends("s4") is not None

    def test_eviction_em_lote(self) -> None:
        """Múltiplas evições simultâneas quando overflow grande."""
        sf = make_filter(max_sessions=2, ttl=60.0)
        clock = sf._clock

        sf.set_active_backends("s1", frozenset({"a"}))
        clock.advance(1)
        sf.set_active_backends("s2", frozenset({"b"}))
        clock.advance(1)
        # max_sessions=2, mas escrevemos 3 — overflow = 3-2+1 = 2 evictions
        sf.set_active_backends("s3", frozenset({"c"}))
        clock.advance(1)
        sf.set_active_backends("s4", frozenset({"d"}))

        assert sf.session_count() == 2
        assert sf.active_backends("s1") is None  # evicted (deadline mais antigo)
        assert sf.active_backends("s2") is None  # evicted (segundo mais antigo)
        assert sf.active_backends("s3") is not None
        assert sf.active_backends("s4") is not None

    def test_read_renova_deadline(self) -> None:
        """Sessão lida recentemente tem deadline posterior — não é evicta primeiro."""
        sf = make_filter(max_sessions=2, ttl=60.0)
        clock = sf._clock

        sf.set_active_backends("s1", frozenset({"a"}))
        clock.advance(10)
        sf.set_active_backends("s2", frozenset({"b"}))
        # Renova s1 via active_backends (read atualiza deadline)
        clock.advance(5)
        sf.active_backends("s1")
        # Agora s2 tem deadline mais antigo que s1
        clock.advance(10)
        sf.set_active_backends("s3", frozenset({"c"}))

        assert sf.session_count() == 2
        assert sf.active_backends("s1") is not None  # renovada
        assert sf.active_backends("s2") is None      # evicted
        assert sf.active_backends("s3") is not None

    def test_session_unica_nunca_evicta_por_si(self) -> None:
        """Sessão sozinha nunca é removida por overflow."""
        sf = make_filter(max_sessions=1, ttl=60.0)
        sf.set_active_backends("s1", frozenset({"a"}))
        sf.set_active_backends("s2", frozenset({"b"}))

        assert sf.session_count() == 1
        assert sf.active_backends("s1") is None  # evicted
        assert sf.active_backends("s2") is not None
