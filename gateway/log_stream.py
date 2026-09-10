"""Broadcaster de eventos de log para o console ao vivo do dashboard (Fase 7).

Módulo standalone, sem dependência do resto do Gateway: qualquer processor
do structlog chama ``log_broadcaster.publish(event_dict)`` e o evento é
replicado a todo cliente SSE conectado em ``GET /api/logs/stream``
(``gateway/http_server.py``). Não é um sistema de log persistente — guarda
só um pequeno buffer circular de replay para quem acabou de conectar.

Integração necessária em ``gateway/logging.py`` (não incluído aqui: este
projeto não tinha esse arquivo nos uploads revisados) — adicionar
``broadcast_processor`` à cadeia de ``processors=[...]`` do
``structlog.configure(...)``, ANTES do renderer final
(JSONRenderer/ConsoleRenderer), porque ele precisa do ``event_dict`` ainda
como dict:

    from gateway.log_stream import broadcast_processor
    structlog.configure(
        processors=[
            ...,  # processors existentes (timestamper, etc.)
            broadcast_processor,   # <- adicionar aqui
            structlog.processors.JSONRenderer(),  # ou o renderer atual
        ],
        ...,
    )
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from typing import Any

# Quantas linhas recentes são reenviadas para quem conecta agora — contexto
# imediato pro console não abrir vazio, sem virar um histórico de verdade.
REPLAY_BUFFER_SIZE = 200

# Cap de segurança por assinante: se um consumidor lento não drena a fila,
# descarta os eventos mais antigos em vez de crescer sem limite de memória.
QUEUE_MAXSIZE = 1000


class LogBroadcaster:
    """Publica eventos de log estruturado para assinantes SSE em tempo real."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._replay: deque[str] = deque(maxlen=REPLAY_BUFFER_SIZE)
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Registra o event loop do Gateway (chamado uma vez no startup).

        ``structlog`` roda os processors de forma síncrona e pode até ser
        chamado antes de qualquer loop existir (logs muito cedo no boot) —
        o loop é guardado explicitamente para agendar a publicação nele com
        segurança via ``call_soon_threadsafe``, funcionando mesmo se
        ``publish`` for chamado de fora do loop principal.
        """
        self._loop = loop

    def publish(self, event_dict: dict[str, Any]) -> None:
        """Serializa e distribui um evento de log a todos os assinantes.

        Chamado de dentro de um processor do structlog: precisa ser rápido
        e NUNCA lançar — um log quebrado não pode derrubar o logging real
        nem a request que o originou.
        """
        try:
            line = json.dumps(_json_safe(event_dict), default=str)
        except Exception:
            return
        self._replay.append(line)
        if self._loop is None:
            return
        for queue in list(self._subscribers):
            self._loop.call_soon_threadsafe(_offer, queue, line)

    async def subscribe(self) -> tuple[asyncio.Queue[str], list[str]]:
        """Registra um novo assinante; devolve a fila e o replay recente."""
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        self._subscribers.add(queue)
        return queue, list(self._replay)

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        self._subscribers.discard(queue)


def _offer(queue: "asyncio.Queue[str]", line: str) -> None:
    """Enfileira sem bloquear; descarta o item mais antigo se a fila está cheia."""
    if queue.full():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
    try:
        queue.put_nowait(line)
    except asyncio.QueueFull:
        pass


def _json_safe(event_dict: dict[str, Any]) -> dict[str, Any]:
    """Normaliza campos que o ``json`` padrão não serializa direto."""
    safe: dict[str, Any] = {}
    for key, value in event_dict.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            safe[key] = value
        else:
            safe[key] = str(value)
    safe.setdefault("timestamp", time.time())
    safe.setdefault("level", safe.get("level", "info"))
    return safe


# Instância única do processo — importada tanto pelo hookup de logging
# (gateway/logging.py) quanto pela rota SSE (gateway/http_server.py).
log_broadcaster = LogBroadcaster()


def broadcast_processor(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Processor do structlog: publica uma cópia do evento e deixa passar.

    Retorna ``event_dict`` inalterado — é um processor "pass-through", só
    espiona o evento pra replicar no console; não interfere no pipeline de
    log normal (arquivo/stdout continuam funcionando exatamente como antes).
    """
    log_broadcaster.publish(dict(event_dict))
    return event_dict
