"""Configuração do logging estruturado do Gateway (structlog).

Todo log emitido pelos módulos do Gateway passa por ``structlog.get_logger``
e herda automaticamente o contexto da requisição em andamento: o HttpServer
faz ``bind_contextvars(request_id=...)`` no início de cada POST /mcp e o
processor ``merge_contextvars`` anexa esse ``request_id`` a cada evento de log
gerado dentro da mesma task asyncio — sem precisar passar o id como parâmetro.
"""

import logging

import structlog

from gateway.log_stream import broadcast_processor


def configure_logging(level: int = logging.INFO, force: bool = False) -> None:
    """Configura structlog (contextvars + nível) e o logging stdlib base.

    O renderer ConsoleRenderer formata eventos legíveis no terminal local.
    Os logs de bibliotecas (uvicorn etc.) seguem pelo logging stdlib.

    ``force`` é repassado a ``logging.basicConfig``.

    Modo biblioteca (padrão): ``force=False`` — não destrói uma configuração
    de logging já existente no processo hospedeiro.

    Modo aplicação standalone: quem controla o processo (ex.: ``main.py``)
    deve chamar explicitamente ``configure_logging(force=True)`` para
    garantir que a configuração do Gateway prevaleça.
    """
    logging.basicConfig(level=level, format="%(message)s", force=force)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            broadcast_processor,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )