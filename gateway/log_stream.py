"""Broadcaster de eventos de log para o console ao vivo do dashboard (Fase 7).

Módulo standalone, sem dependência do resto do Gateway: qualquer processor
do structlog chama ``log_broadcaster.publish(event_dict)`` e o evento é
replicado a todo cliente SSE conectado em ``GET /api/logs/stream``
(``gateway/http_server.py``). Não é um sistema de log persistente — guarda
só um pequeno buffer circular de replay para quem acabou de conectar.

Integração (JÁ FEITA em ``gateway/logging.py``): ``broadcast_processor`` entra
na cadeia de ``processors=[...]`` do ``structlog.configure(...)``, ANTES do
renderer final (JSONRenderer/ConsoleRenderer), porque ele precisa do
``event_dict`` ainda como dict — qualquer nova configuração de logging do
Gateway deve preservar essa ordem.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import MutableMapping
from collections import deque
from typing import Any

# Quantas linhas recentes são reenviadas para quem conecta agora — contexto
# imediato pro console não abrir vazio, sem virar um histórico de verdade.
REPLAY_BUFFER_SIZE = 200

# Cap de segurança por assinante: se um consumidor lento não drena a fila,
# descarta os eventos mais antigos em vez de crescer sem limite de memória.
QUEUE_MAXSIZE = 1000

# Limite total de assinantes simultâneos no console ao vivo.
# Proteção contra DoS: cada subscriber consome ~32KB de refs;
# 1000+ conexões = memory exhaustion. Ajustar conforme memória disponível.
MAX_SUBSCRIBERS = 100


def _monotonic() -> float:
    """Atalho para ``time.monotonic()"""
    return time.monotonic()


# Campos que compõem a chave de deduplicação — agrupa eventos idênticos
# (mesmo evento, mesmo backend, mesma rota) num único log com contador.
_DEDUP_KEY_FIELDS = ("event", "backend", "route")

# Intervalo (segundos) entre flushs de eventos repetidos no broadcast.
_DEDUP_FLUSH_INTERVAL = 5.0


def _dedup_key(event_dict: dict[str, Any]) -> str:
    """Gera uma chave de dedup a partir dos campos estáveis do evento."""
    parts = [str(event_dict.get(f, "")) for f in _DEDUP_KEY_FIELDS]
    return "|".join(parts)


class LogBroadcaster:
    """Publica eventos de log estruturado para assinantes SSE em tempo real."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._replay: deque[str] = deque(maxlen=REPLAY_BUFFER_SIZE)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._dedup_counts: dict[str, int] = {}
        self._dedup_first_seen: dict[str, float] = {}
        self._dedup_last_flush: float = 0.0

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

        Deduplicação: a primeira ocorrência de cada evento é transmitida
        imediatamente. Ocorrências seguintes acumulam contador e são
        transmitidas como resumo (``_repeated``) a cada
        ``_DEDUP_FLUSH_INTERVAL`` segundos.
        """
        try:
            line = json.dumps(_json_safe(event_dict), default=str)
        except Exception:
            return
        key = _dedup_key(event_dict)
        now = _monotonic()
        self._dedup_counts[key] = self._dedup_counts.get(key, 0) + 1
        if key not in self._dedup_first_seen:
            self._dedup_first_seen[key] = now
        # Primeira ocorrência: passa direto.
        if self._dedup_counts[key] == 1:
            self._raw_publish(line)
            self._dedup_last_flush = now
            return
        # Ocorrências seguintes: aguarda flush periódico.
        if now - self._dedup_last_flush >= _DEDUP_FLUSH_INTERVAL:
            self._flush_dedup()

    def _raw_publish(self, line: str) -> None:
        """Publica uma linha JSON bruta sem deduplicação (interno)."""
        self._replay.append(line)
        if self._loop is None:
            return
        for queue in list(self._subscribers):
            self._loop.call_soon_threadsafe(_offer, queue, line)

    def _flush_dedup(self) -> None:
        """Emite resumo dos eventos repetidos e reseta contadores."""
        now = _monotonic()
        for key, count in list(self._dedup_counts.items()):
            if count > 1:
                parts = key.split("|")
                summary: dict[str, Any] = {
                    "event": "_repeated",
                    "original_event": parts[0] if parts else "",
                    "count": count,
                }
                if len(parts) > 1 and parts[1]:
                    summary["backend"] = parts[1]
                if len(parts) > 2 and parts[2]:
                    summary["route"] = parts[2]
                try:
                    line = json.dumps(_json_safe(summary), default=str)
                except Exception:
                    continue
                self._raw_publish(line)
        self._dedup_counts.clear()
        self._dedup_first_seen.clear()
        self._dedup_last_flush = now

    async def subscribe(self) -> tuple[asyncio.Queue[str], list[str]]:
        """Registra um novo assinante; devolve a fila e o replay recente.

        Raises:
            RuntimeError: se o limite de subscribers simultâneos foi atingido.
        """
        if len(self._subscribers) >= MAX_SUBSCRIBERS:
            raise RuntimeError(
                f"limite de {MAX_SUBSCRIBERS} conexões de log simultâneas atingido"
            )
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


_MAX_JSON_SAFE_DEPTH = 5
"""Teto de profundidade da conversão recursiva de ``_json_safe``: contexto de
log que passar disso vira ``str()``. Protege o broadcaster de referências
circulares (dict que contém a si mesmo) e de estruturas patológicas — um log
quebrado não pode derrubar o logging (ver ``LogBroadcaster.publish``)."""


def _json_safe_value(value: Any, depth: int = 0) -> Any:
    """Converte um valor para algo serializável em JSON, recursivamente.

    Primitivos passam direto; dict/list/tuple/set são percorridos (dict/list
    chegam ao ``JSON.parse`` do console ao vivo como objeto/array navegável,
    não como a repr Python em string); chaves de dict viram ``str`` (exigência
    do JSON); folhas exóticas (datetime etc.) e o além do teto de profundidade
    caem no ``str(value)``.
    """
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if depth >= _MAX_JSON_SAFE_DEPTH:
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v, depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe_value(item, depth + 1) for item in value]
    return str(value)


def _json_safe(event_dict: dict[str, Any]) -> dict[str, Any]:
    """Normaliza os campos de um evento de log para serialização JSON.

    A conversão é RECURSIVA (ver ``_json_safe_value``): contexto estruturado
    aninhado — ex.: ``per_backend_chars`` do diagnóstico de tools — chega ao
    console ao vivo como objeto JSON, e não como string de repr Python.

    SEM defaults de ``timestamp``/``level``: na cadeia real (ver
    ``gateway/logging.py``) este módulo roda DEPOIS de ``add_log_level`` e do
    ``TimeStamper(fmt="iso")``, então os dois campos já chegam preenchidos —
    ``level`` como string, ``timestamp`` como ISO-8601 string (não epoch; o
    ``fmtTs`` do dashboard trata o ISO direto). Os ``setdefault`` antigos
    eram inalcançáveis pelo único produtor (``broadcast_processor``) e o
    default epoch era enganoso sobre o formato real do campo.
    """
    return {str(k): _json_safe_value(v) for k, v in event_dict.items()}


# Instância única do processo — importada tanto pelo hookup de logging
# (gateway/logging.py) quanto pela rota SSE (gateway/http_server.py).
log_broadcaster = LogBroadcaster()


def broadcast_processor(
    logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Processor do structlog: publica uma cópia do evento e deixa passar.

    Retorna ``event_dict`` inalterado — é um processor "pass-through", só
    espiona o evento pra replicar no console; não interfere no pipeline de
    log normal (arquivo/stdout continuam funcionando exatamente como antes).

    Assinatura segue o protocolo ``structlog.typing.Processor``
    (``MutableMapping`` de entrada/saída) — anotar ``dict`` rejeita o
    processor na lista de ``structlog.configure`` (dict é mais estreito que
    ``MutableMapping`` e parâmetros exigem contravariância).
    """
    log_broadcaster.publish(dict(event_dict))
    return event_dict
