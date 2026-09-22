"""Configuração do logging estruturado do Gateway (structlog).

Todo log dos módulos do Gateway passa por ``structlog.get_logger`` e herda o
contexto da requisição em andamento: o HttpServer faz
``bind_contextvars(request_id=...)`` no início de cada ``POST /mcp`` e o
processor ``merge_contextvars`` anexa esse ``request_id`` a cada evento gerado
dentro da mesma task asyncio, sem precisar propagar o id manualmente.

A cadeia de ``processors`` inclui ``broadcast_processor``
(:mod:`gateway.log_stream`), que espelha cada evento para o console ao vivo do
dashboard antes do renderer final, sem alterar o pipeline de log padrão.

O modo JSON pode ser ativado via ``MCP_GATEWAY_LOG_FORMAT=json`` — útil em
produção com ELK/Loki/Grafana onde cada linha é parseada como JSON.
"""

import logging
import os

import structlog

from gateway.log_stream import broadcast_processor


def configure_logging(level: int = logging.INFO, force: bool = False) -> None:
    """Configura o structlog (contextvars + nível) e o logging stdlib base.

    O ``ConsoleRenderer`` formata eventos legíveis no terminal local, enquanto
    ``MCP_GATEWAY_LOG_FORMAT=json`` ativa o ``JSONRenderer`` para produção
    (ELK/Loki/Grafana). O ``broadcast_processor`` espelha tudo pro console
    ao vivo do dashboard independente do renderer.
    """
    log_format = os.environ.get("MCP_GATEWAY_LOG_FORMAT", "console").strip().lower()
    if log_format == "json":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()
    logging.basicConfig(level=level, format="%(message)s", force=force)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            broadcast_processor,
            renderer,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )
