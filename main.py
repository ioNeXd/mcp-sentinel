"""Ponto de entrada do MCP Gateway: sobe todos os componentes e desliga tudo.

Orquestra o ciclo de vida completo do processo do Gateway em modo aplicação
standalone:

1. valida a configuração runtime (porta e host via env, config.json);
2. sobe backends (McpServer.start), Health Monitor e Session Purger;
3. serve o HTTP via uvicorn até receber o sinal de shutdown;
4. desliga tudo graciosamente no ``finally`` — sem deixar processos órfãos
   nem vazar traceback na saída do Ctrl+C.

Sinais de shutdown: o uvicorn instala os handlers de SIGINT/SIGTERM e apenas
marca ``should_exit`` (não sobrescrevemos esses sinais, para não conflitar com
o próprio shutdown dele); no Windows completamos com o CTRL_BREAK (SIGBREAK)
via ``_install_sigbreak_handler``.
"""

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

import structlog
import uvicorn

from gateway.backend_manager import BackendManager
from gateway.config import load_config
from gateway.errors import BackendError
from gateway.health_monitor import HealthMonitor
from gateway.http_server import create_app
from gateway.logging import configure_logging
from gateway.registries import PromptRegistry, ResourceRegistry, ToolRegistry
from gateway.server import McpServer
from gateway.sessions import SessionFilter, SessionPurger

DEFAULT_PORT = 8080
DEFAULT_HOST = "127.0.0.1"
DEFAULT_CONFIG_PATH = "config/config.json"

logger = structlog.get_logger("main")


def _configured_host() -> str:
    """Retorna o host de bind (env ``MCP_GATEWAY_HOST``), com fallback local seguro.

    Vazio ou só espaços cai no default ``127.0.0.1`` — nunca expõe o Gateway
    além da máquina local por acidente de configuração.
    """
    return os.environ.get("MCP_GATEWAY_HOST", DEFAULT_HOST).strip() or DEFAULT_HOST


def _install_sigbreak_handler(server: uvicorn.Server) -> None:
    """No Windows, faz CTRL_BREAK (SIGBREAK) também disparar o shutdown gracioso.

    O uvicorn instala handlers só para SIGINT/SIGTERM; CTRL_BREAK — usado por
    ferramentas e gerenciadores de serviço no Windows — cairia no handler
    default do Python, resultando em término duro sem passar pelo ``finally``.
    O handler apenas marca ``should_exit``, de modo que o loop do uvicorn
    encerra pelo mesmo caminho do Ctrl+C. Em sistemas sem SIGBREAK é um no-op.
    """
    if not hasattr(signal, "SIGBREAK"):
        return

    def _handle_sigbreak(signum: int, frame: Any) -> None:
        logger.info("sigbreak_recebido", detail="encerrando graciosamente")
        server.should_exit = True

    signal.signal(signal.SIGBREAK, _handle_sigbreak)


async def main() -> int:
    """Sobe backends, Health Monitor e o HTTP; desliga tudo graciosamente.

    Retorna o exit code do processo: ``0`` em desligamento normal (inclusive
    Ctrl+C) e ``1`` em falha de configuração ou de startup dos backends.

    Em modo aplicação standalone força a configuração de logging do Gateway a
    prevalecer sobre qualquer ``basicConfig`` pré-existente no processo
    (``force=True`` — ver docstring de ``configure_logging``).

    Se o startup dos backends falhar de forma inesperada (bug real, não
    modelado como ``BackendError``), ``stop_all`` roda ANTES do log para que
    nenhum processo/conexão de backend fique órfão e o log final reflita o
    estado pós-cleanup.
    """
    configure_logging(logging.INFO, force=True)
    port_raw = os.environ.get("MCP_GATEWAY_PORT", str(DEFAULT_PORT))
    try:
        port = int(port_raw)
    except ValueError:
        logger.error("MCP_GATEWAY_PORT inválido", value=port_raw)
        return 1
    if not 1 <= port <= 65535:
        logger.error("MCP_GATEWAY_PORT fora do intervalo válido (1-65535)", value=port_raw)
        return 1

    config_path = Path(os.environ.get("MCP_GATEWAY_CONFIG", DEFAULT_CONFIG_PATH))
    try:
        config = load_config(config_path)
    except ValueError as exc:
        logger.error("falha ao carregar configuração", path=str(config_path), error=str(exc))
        return 1

    if config.auth_token is None:
        logger.warning(
            "rodando SEM autenticação — defina auth_token no config.json para expor além da máquina local"
        )

    registries = (ToolRegistry(), ResourceRegistry(), PromptRegistry())
    backend_manager = BackendManager(config, registries)
    session_filter = SessionFilter(ttl_seconds=config.session_ttl_seconds)
    mcp_server = McpServer(backend_manager, registries, session_filter=session_filter)
    try:
        await mcp_server.start()
    except BackendError as exc:
        logger.error("falha ao iniciar os backends", error=str(exc))
        return 1
    except Exception:
        await backend_manager.stop_all()
        logger.exception("falha inesperada ao iniciar os backends")
        return 1

    health_monitor = HealthMonitor(
        backend_manager, interval_seconds=config.health_check_interval_seconds
    )
    health_monitor.start()
    session_purger = SessionPurger(session_filter)
    session_purger.start()

    app = create_app(
        mcp_server,
        auth_token=config.auth_token,
        max_payload_bytes=config.max_payload_bytes,
    )
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=_configured_host(),
            port=port,
            log_level="warning",
            # O access log do uvicorn emite a URL COMPLETA da request (incluindo
            # ``?token=...``), redundante com o ``http_request_completed``
            # estruturado. Desligado para o token de query nunca vazar em log.
            access_log=False,
        )
    )
    # Fase 7 — botão "Sair do MCP" no dashboard: a rota POST /api/shutdown (em
    # gateway/http_server.py) só marca server.should_exit = True, disparando
    # o MESMO caminho de shutdown gracioso do Ctrl+C (finally abaixo cuida do
    # resto). Guardada em app.state porque o app é criado antes do Server.
    app.state.uvicorn_server = server

    _install_sigbreak_handler(server)
    try:
        await server.serve()
    except asyncio.CancelledError:
        logger.info("gateway_shutdown_interrupted", reason="task cancelada pelo event loop")
    except KeyboardInterrupt:
        logger.info("gateway_shutdown_interrupted", reason="KeyboardInterrupt")
    finally:
        await _graceful_shutdown(health_monitor, session_purger, mcp_server)
    return 0


async def _graceful_shutdown(
    health_monitor: HealthMonitor,
    session_purger: SessionPurger,
    mcp_server: McpServer,
) -> None:
    """Desliga os componentes em ordem, tolerando cancelamento e erros.

    Ordem: (1) Health Monitor (para de checar/reiniciar); (2) Session Purger;
    (3) backends — ``stop_all`` encerra cada processo filho (stdin fechado →
    terminate → kill), sem deixar órfãos. Cada etapa é isolada: uma falha —
    inclusive o cancelamento do ``main()`` no meio do shutdown — não impede a
    seguinte de rodar.

    ``CancelledError`` recebido em qualquer etapa é registrado e RE-LANÇADO ao
    final (nunca engolido): engoli-lo mascararia um desligamento em andamento
    do event loop, mas propagá-lo no meio impediria os backends de serem
    finalizados. Emite ``gateway_shutdown_complete`` quando todos os passos
    terminam.
    """
    cancelled = False
    try:
        await health_monitor.stop()
    except asyncio.CancelledError:
        logger.warning("shutdown_health_monitor_cancelado")
        cancelled = True
    except Exception as exc:
        logger.error("falha ao parar o health monitor", error=str(exc))
    try:
        await session_purger.stop()
    except asyncio.CancelledError:
        logger.warning("shutdown_session_purger_cancelado")
        cancelled = True
    except Exception as exc:
        logger.error("falha ao parar o session purger", error=str(exc))
    try:
        await mcp_server.stop()
    except asyncio.CancelledError:
        logger.warning("shutdown_backends_cancelado")
        cancelled = True
    except Exception as exc:
        logger.error("falha ao parar os backends", error=str(exc))
    logger.info("gateway_shutdown_complete")
    if cancelled:
        raise asyncio.CancelledError


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except (KeyboardInterrupt, asyncio.CancelledError):
        # Desfecho normal do Ctrl+C após o cleanup completo do main():
        # encerra com código 0 e SEM traceback.
        sys.exit(0)
