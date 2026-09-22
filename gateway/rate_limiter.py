"""Rate limiter simples in-memory por chave (ex: IP).

Sliding window counters — sem dependências externas. Cada janela é um
``deque`` de timestamps; entries fora da janela são descartadas no check.

Uso:
    limiter = RateLimiter(max_requests=10, window_seconds=60)
    if not limiter.allow(client_ip):
        return 429
"""

import time
from collections import defaultdict, deque
from typing import Callable


class RateLimiter:
    """Rate limiter por chave com sliding window.

    Args:
        max_requests: máximo de requests permitidos na janela.
        window_seconds: tamanho da janela em segundos.
        clock: callable que retorna tempo (default ``time.monotonic``).
    """

    def __init__(
        self,
        max_requests: int,
        window_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._clock = clock
        self._windows: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str) -> bool:
        """ Retorna ``True`` se o request é permitido, ``False`` se excedeu o limite. """
        now = self._clock()
        window = self._windows[key]
        cutoff = now - self.window_seconds
        # Descarta entries fora da janela
        while window and window[0] <= cutoff:
            window.popleft()
        if len(window) >= self.max_requests:
            return False
        window.append(now)
        return True
