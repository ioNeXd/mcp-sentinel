"""Ponto de entrada do MCP Gateway (Fase 2 do ROADMAP)."""

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
    """Retorna o bind configurado, mantendo o default local seguro."""
    return os.environ.get("MCP_GATEWAY_HOST", DEFAULT_HOST).strip() or DEFAULT_HOST


def _install_sigbreak_handler(server: uvicorn.Server) -> None:
    """No Windows, CTRL_BREAK (SIGBREAK) também dispara o shutdown gracioso.

    O uvicorn instala handlers só para SIGINT/SIGTERM; CTRL_BREAK (usado por
    ferramentas e gerenciadores de serviço no Windows) cairia no handler
    default do Python — término duro, sem passar pelo ``finally``. O handler
    apenas marca ``should_exit`` e o loop do uvicorn encerra pelo mesmo
    caminho do Ctrl+C. Em outros sistemas não há SIGBREAK: nada a fazer.
    """
    if not hasattr(signal, "SIGBREAK"):
        return

    def _handle_sigbreak(signum: int, frame: Any) -> None:
        logger.info("sigbreak_recebido", detail="encerrando graciosamente")
        server.should_exit = True

    signal.signal(signal.SIGBREAK, _handle_sigbreak)


async def main() -> int:
    """Sobe backends, health monitor e o HTTP; desliga tudo graciosamente."""
    configure_logging(logging.INFO)
    port_raw = os.environ.get("MCP_GATEWAY_PORT", str(DEFAULT_PORT))
    try:
        port = int(port_raw)
    except ValueError:
        logger.error("MCP_GATEWAY_PORT inválido", value=port_raw)
        return 1
    # 3.2 — porta fora do intervalo válido falharia dentro do uvicorn com um
    # erro obscuro; valida logo com mensagem clara e exit code consistente.
    if not 1 <= port <= 65535:
        logger.error(
            "MCP_GATEWAY_PORT fora do intervalo válido (1-65535)", value=port_raw
        )
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
        # 3.3 — falha inesperada (bug real, não modelada como BackendError):
        # nenhum processo/conexão de backend pode ficar órfão. O stop_all é
        # tolerante a falha por backend (ver BackendManager._teardown_client);
        # o cleanup roda ANTES do log para que o log final reflita o estado
        # pós-cleanup. A exceção original segue no logger.exception abaixo.
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
            # Access log do uvicorn emite a URL COMPLETA da request (incluindo
            # ``?token=...``) e é redundante com o ``http_request_completed``
            # estruturado (que não inclui URL). Desligado para o token de query
            # nunca vazar em log — ver nota de segurança no README.
            access_log=False,
        )
    )

    # Graceful shutdown (Fase 2): o uvicorn instala os handlers de SIGINT/SIGTERM
    # e, ao receber o sinal, apenas marca ``should_exit = True`` — o ``serve()``
    # retorna sem exceção. Por isso NÃO sobrescrevemos SIGINT/SIGTERM (isso
    # conflitaria com o próprio shutdown do uvicorn); apenas completamos com o
    # SIGBREAK do Windows (ver _install_sigbreak_handler). A ordem de
    # desligamento fica no ``finally``:
    # 1) health monitor (para de checar/reiniciar); 2) backends — ``stop_all``
    # encerra cada processo filho (stdin fechado → terminate → kill), sem
    # deixar órfãos; 3) o processo sai normalmente.
    _install_sigbreak_handler(server)
    try:
        await server.serve()
    except asyncio.CancelledError:
        # Cancelamento do ``main()`` pelo event loop em encerramento (ex.:
        # ``asyncio.run`` derrubando a task principal no shutdown, ou Ctrl+C
        # no Unix quando o loop propaga o cancelamento). É o mecanismo de
        # parada, não um erro: engolido APENAS aqui no entrypoint, depois de
        # o ``finally`` completar o cleanup — nunca nas camadas de baixo.
        logger.info("gateway_shutdown_interrupted", reason="task cancelada pelo event loop")
    except KeyboardInterrupt:
        # ``serve()`` pode propagar KeyboardInterrupt: o uvicorn captura o
        # SIGINT/SIGBREAK durante ``serve()``, mas ao sair de
        # ``capture_signals()`` REEMITE o sinal capturado com o handler
        # original restaurado — no Windows isso chega aqui como
        # KeyboardInterrupt. Sem este ``except``, o desfecho seria um
        # traceback de KeyboardInterrupt na tela mesmo com o shutdown
        # gracioso completando (os backends são finalizados no ``finally``
        # de qualquer forma; o que faltava era silenciar o traceback).
        logger.info("gateway_shutdown_interrupted", reason="KeyboardInterrupt")
    finally:
        await _graceful_shutdown(health_monitor, session_purger, mcp_server)
    return 0


async def _graceful_shutdown(
    health_monitor: HealthMonitor,
    session_purger: SessionPurger,
    mcp_server: McpServer,
) -> None:
    """Ordem de desligamento, tolerante a cancelamento/erros.

    1) health monitor (para de checar/reiniciar); 2) session purger;
    3) backends — ``stop_all`` encerra cada processo filho (stdin fechado →
    terminate → kill), sem deixar órfãos. Cada etapa é isolada: uma falha
    (incluído cancelamento do ``main()`` no meio do shutdown) não impede a
    seguinte de rodar. Emite ``gateway_shutdown_complete`` quando todos os
    passos terminam.
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
        # Desfecho normal do Ctrl+C após o cleanup completo do ``main()``
        # (ver comentários em main()): encerra com código 0 e SEM traceback.
        sys.exit(0)
