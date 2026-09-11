"""Testes do shutdown gracioso do Gateway (regressão do Ctrl+C, Fase 2).

Cobrem os dois caminhos pelos quais o Ctrl+C pode chegar ao ``main()``:

1. ``KeyboardInterrupt`` propagado pelo uvicorn (ele captura o SIGINT durante
   ``serve()`` e REEMITE o sinal ao sair, com o handler original restaurado);
2. cancelamento da task principal pelo event loop (``CancelledError``).

Em ambos, o desfecho esperado é: ``gateway_shutdown_complete`` nos logs,
backends finalizados (``backend_manager_stopped``) e NENHUM traceback na
saída — o ``CancelledError`` do health monitor é o desfecho esperado do
cancelamento, não um erro.
"""

import asyncio
import contextlib
import io
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import FAKE_BACKEND_PATH, configure_quiet_structlog, make_fake_manager
from gateway.health_monitor import HealthMonitor
from gateway.registries import PromptRegistry, ResourceRegistry, ToolRegistry
from gateway.server import McpServer
from gateway.sessions import SessionFilter, SessionPurger

ROOT = Path(__file__).resolve().parents[1]


class _FakeUvicornServer:
    """Substituto do ``uvicorn.Server`` que simula um Ctrl+C real.

    ``serve()`` espera um instante (para o health monitor rodar) e levanta
    ``KeyboardInterrupt`` — exatamente o que o uvicorn 0.52 faz ao reemitir o
    SIGINT capturado no fim de ``capture_signals()``.
    """

    should_exit = False
    force_exit = False

    def __init__(self, _config: Any) -> None:
        pass

    async def serve(self) -> None:
        await asyncio.sleep(0.05)
        raise KeyboardInterrupt


@pytest.mark.asyncio
async def test_main_shutdown_completo_sem_traceback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ctrl+C real (KeyboardInterrupt do serve()) → sem traceback, com log de conclusão."""
    import main as main_module

    async def fake_serve(self: Any) -> None:  # pragma: no cover - ver _FakeUvicornServer
        raise KeyboardInterrupt

    monkeypatch.setattr("uvicorn.Server", _FakeUvicornServer)
    monkeypatch.setattr(main_module, "_install_sigbreak_handler", lambda _server: None)
    monkeypatch.setenv("MCP_GATEWAY_PORT", "8197")
    monkeypatch.setenv("MCP_GATEWAY_CONFIG", "config/config.json")

    stdout, stderr = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = await main_module.main()
    finally:
        configure_quiet_structlog()  # restaura o baseline silencioso dos testes

    output = stdout.getvalue() + stderr.getvalue()
    assert exit_code == 0
    assert "gateway_shutdown_interrupted" in output
    assert "backend_manager_stopped" in output  # backends finalizados
    assert "gateway_shutdown_complete" in output
    assert "Traceback" not in output, f"traceback vazou na saída:\n{output[-2000:]}"
    assert "CancelledError" not in output


@pytest.mark.asyncio
async def test_graceful_shutdown_propaga_cancelamento_do_chamador(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelamento do main() DURANTE o shutdown: propaga, nunca é engolido.

    Exceções comuns são isoladas por etapa (a próxima etapa roda), mas
    ``CancelledError`` só propaga depois de todas as etapas — engoli-lo
    mascararia um desligamento em andamento do event loop, mas propagá-lo no
    meio impediria os backends de serem finalizados.
    """
    from main import _graceful_shutdown

    manager, _factory = make_fake_manager(("backend-a",))
    monitor = HealthMonitor(manager, interval_seconds=3600.0)
    monitor.start()
    await manager.start_all()
    mcp_server = McpServer(manager, (ToolRegistry(), ResourceRegistry(), PromptRegistry()))
    # session_purger não iniciado: seu stop() é um no-op limpo (task is None),
    # servindo só para preencher a etapa intermediária do _graceful_shutdown.
    session_purger = SessionPurger(SessionFilter(ttl_seconds=3600.0))

    stop_calls: list[str] = []

    async def stop_lento() -> None:
        await asyncio.Event().wait()  # nunca completa: mantém stop() cancelável

    monkeypatch.setattr(monitor, "stop", stop_lento)
    monkeypatch.setattr(mcp_server, "stop", lambda: stop_calls.append("mcp") or asyncio.sleep(0))

    shutdown_task = asyncio.create_task(_graceful_shutdown(monitor, session_purger, mcp_server))
    await asyncio.sleep(0.05)  # _graceful_shutdown já está preso no stop() do monitor
    shutdown_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await shutdown_task

    assert stop_calls == ["mcp"]  # a etapa seguinte roda antes de propagar
    # Cleanup do teste: para o monitor e os backends de verdade.
    monkeypatch.undo()
    await monitor.stop()
    await manager.stop_all()


def _run_gateway_subprocess() -> str:
    """Sobe o gateway real, espera /health ok, envia o sinal de shutdown.

    Windows: CTRL_BREAK (grupo próprio de processo) — o mesmo mecanismo
    validado no smoke test da Fase 2; o CTRL_C_EVENT não é entregue a grupos.
    POSIX: SIGINT direto (o Ctrl+C de verdade).

    Usa um config PRÓPRIO (só o backend stdio fake) em vez do
    ``config/config.json``: o teste valida o shutdown, não o conteúdo do
    config do usuário — que pode ter backends remotos fora do ar (o /health
    ficaria 'degraded' e o critério de prontidão nunca chegaria).
    """
    import json
    import tempfile
    import urllib.request

    config_path = Path(tempfile.mkstemp(suffix=".json", prefix="gw-shutdown-")[1])
    config_path.write_text(
        json.dumps(
            {
                "backends": [
                    {
                        "name": "backend-a",
                        "command": sys.executable,
                        "args": [str(FAKE_BACKEND_PATH)],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    env = {
        **os.environ,
        "MCP_GATEWAY_PORT": "8123",
        "MCP_GATEWAY_CONFIG": str(config_path),
        "PYTHONUNBUFFERED": "1",
    }
    proc = subprocess.Popen(  # noqa: S603 — caminhos fixos do projeto
        [sys.executable, "main.py"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=creationflags,
    )
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(  # noqa: S310 — localhost
                    "http://127.0.0.1:8123/health", timeout=1
                ) as resp:
                    if json.loads(resp.read().decode()).get("status") == "ok":
                        break
            except Exception:  # noqa: BLE001 — ainda subindo
                time.sleep(0.3)
        else:
            pytest.fail("/health não ficou ok em 30s")

        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)

        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pytest.fail("gateway não encerrou em 30s após o sinal")
        return proc.stdout.read() if proc.stdout else ""
    finally:
        if proc.poll() is None:
            proc.kill()
        with contextlib.suppress(OSError):
            config_path.unlink()  # config temporário do teste


def test_gateway_subprocess_shutdown_sem_traceback() -> None:
    """E2E: processo real recebe o sinal de shutdown; saída limpa e completa."""
    output = _run_gateway_subprocess()
    assert "Traceback" not in output, f"traceback vazou na saída:\n{output[-3000:]}"
    assert "CancelledError" not in output
    assert "gateway_shutdown_complete" in output
    assert "backend_manager_stopped" in output  # backends finalizados antes de sair
