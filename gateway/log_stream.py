"""Broadcaster de eventos de log para o console ao vivo do dashboard (Fase 7).

Módulo standalone, sem dependência do resto do Gateway: o processor
``broadcast_processor`` (registrado na cadeia de ``processors`` em
:mod:`gateway.logging`) chama ``log_broadcaster.publish(event_dict)`` e o
evento é replicado a todo cliente SSE conectado em ``GET /api/logs/stream``
(:mod:`gateway.http_server`).

Não é um sistema de log persistente: mantém apenas um pequeno buffer circular
de replay (:data:`REPLAY_BUFFER_SIZE`) para dar contexto imediato a quem acaba
de conectar. Cada assinante tem uma fila limitada (:data:`QUEUE_MAXSIZE`) que
descarta os eventos mais antigos sob backpressure, nunca crescendo sem limite.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from typing import Any

from structlog.typing import EventDict, WrappedLogger

#: Linhas recentes reenviadas a quem conecta agora (replay de contexto imediato).
REPLAY_BUFFER_SIZE = 200

#: Teto de eventos enfileirados por assinante antes de descartar os mais antigos.
QUEUE_MAXSIZE = 1000


class LogBroadcaster:
    """Publica eventos de log estruturado para assinantes SSE em tempo real."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._replay: deque[str] = deque(maxlen=REPLAY_BUFFER_SIZE)
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Registra o event loop do Gateway (chamado uma vez no startup).

        O structlog roda os processors de forma síncrona e pode ser acionado
        antes de qualquer loop existir (logs muito cedo no boot). Guardar o
        loop explicitamente permite agendar a publicação nele com segurança
        via ``call_soon_threadsafe``, mesmo que ``publish`` seja chamado de
        fora do loop principal. Enquanto o loop não é vinculado, ``publish``
        apenas alimenta o buffer de replay.
        """
        self._loop = loop

    def publish(self, event_dict: dict[str, Any]) -> None:
        """Serializa e distribui um evento de log a todos os assinantes.

        Invocado de dentro de um processor do structlog: precisa ser rápido e
        NUNCA lançar — um log malformado não pode derrubar o pipeline de log
        real nem a requisição que o originou.
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
        """Registra um novo assinante e devolve sua fila e o replay recente."""
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        self._subscribers.add(queue)
        return queue, list(self._replay)

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        """Remove um assinante (idempotente)."""
        self._subscribers.discard(queue)


def _offer(queue: asyncio.Queue[str], line: str) -> None:
    """Enfileira sem bloquear, descartando o item mais antigo se a fila encheu."""
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
    """Normaliza campos que o ``json`` padrão não serializa diretamente."""
    safe: dict[str, Any] = {}
    for key, value in event_dict.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            safe[key] = value
        else:
            safe[key] = str(value)
    safe.setdefault("timestamp", time.time())
    safe.setdefault("level", safe.get("level", "info"))
    return safe


#: Instância única do processo, compartilhada pelo hookup de logging
#: (:mod:`gateway.logging`) e pela rota SSE (:mod:`gateway.http_server`).
log_broadcaster = LogBroadcaster()


def broadcast_processor(
    logger: WrappedLogger, method_name: str, event_dict: EventDict
) -> EventDict:
    """Processor pass-through do structlog: espelha o evento e o repassa.

    Devolve ``event_dict`` inalterado — apenas publica uma cópia para o console
    ao vivo, sem interferir no pipeline de log padrão (arquivo/stdout seguem
    funcionando como antes).
    """
    log_broadcaster.publish(dict(event_dict))
    return event_dict
