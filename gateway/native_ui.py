"""Janela nativa do dashboard via ``pywebview`` (Fase 8 — modo local).

Reaproveita 100% do HTML/CSS/JS que ``gateway.http_server`` já serve — troca
só o "container" que exibe: em vez do navegador padrão do usuário, uma janela
nativa (WebView2 no Windows, WebKit no macOS/Linux). Nada no dashboard em si
precisa saber que está rodando dentro de uma janela nativa.

Este módulo só é importado quando ``MCP_GATEWAY_UI_MODE=native`` (o padrão) e
o pacote ``pywebview`` está instalado — ver ``main.py``. Mantém o import de
``webview`` no escopo do módulo (não dentro de funções) porque, se este
arquivo for importado, o caller (``run_native_mode`` em ``main.py``) já
confirmou a disponibilidade do pacote.
"""

from __future__ import annotations

import threading
import time
import urllib.error
import urllib.request

import structlog
import webview

logger = structlog.get_logger(__name__)

WINDOW_TITLE = "MCP Gateway"
DEFAULT_WIDTH = 1180
DEFAULT_HEIGHT = 780
MIN_SIZE = (760, 480)
READY_TIMEOUT_SECONDS = 20.0
READY_POLL_INTERVAL_SECONDS = 0.3
SHUTDOWN_REQUEST_TIMEOUT_SECONDS = 2.0

_LOADING_HTML = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
body { background:#0f1117; color:#8b91a5; font-family:system-ui,-apple-system,sans-serif;
  display:flex; align-items:center; justify-content:center; height:100vh; margin:0; }
</style></head><body>Iniciando o MCP Gateway…</body></html>"""

_UNREACHABLE_HTML = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
body { background:#0f1117; color:#e05a5a; font-family:system-ui,-apple-system,sans-serif;
  padding:2rem; }
</style></head><body>
<h2>O Gateway não respondeu a tempo.</h2>
<p>Feche esta janela e confira o terminal para ver o erro real.</p>
</body></html>"""


def _wait_until_ready(health_url: str, timeout: float) -> bool:
    """Faz polling em ``GET {health_url}`` até responder (ou estourar o timeout).

    Roda numa thread separada da UI (chamada por ``_poll_and_load``) — um
    ``time.sleep`` aqui nunca trava a janela nativa em si.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=1.0):
                return True
        except (urllib.error.URLError, OSError, TimeoutError):
            time.sleep(READY_POLL_INTERVAL_SECONDS)
    return False


def run_window(dashboard_url: str, shutdown_url: str, token: str | None) -> None:
    """Abre a janela nativa, espera o servidor responder, e navega pra ela.

    Fluxo: a janela abre IMEDIATAMENTE com uma tela de "iniciando" (evita o
    branco/erro de conexão caso o Gateway ainda esteja subindo os backends) —
    em paralelo, uma thread faz polling em ``/health`` e só então chama
    ``window.load_url`` com a URL real (token embutido na query string, igual
    ao dashboard já aceita hoje). Se o timeout estourar, mostra uma tela de
    erro simples em vez de deixar a janela travada na tela de loading pra
    sempre.

    Fechar a janela (evento ``closing``) dispara um POST best-effort em
    ``shutdown_url`` — a MESMA rota ``/api/shutdown`` que o botão "Sair do
    MCP" do próprio dashboard já usa. Isso garante que fechar a janela
    encerra o processo do Gateway inteiro (backends inclusos) em vez de
    deixá-lo órfão rodando em background sem nenhuma UI apontando pra ele.

    Bloqueia até a janela ser fechada (``webview.start()``) — chamado pela
    thread principal do processo (exigência do pywebview, crítica no macOS).
    """
    window = webview.create_window(
        WINDOW_TITLE,
        html=_LOADING_HTML,
        width=DEFAULT_WIDTH,
        height=DEFAULT_HEIGHT,
        min_size=MIN_SIZE,
    )

    def _poll_and_load() -> None:
        health_url = dashboard_url.rstrip("/") + "/health"
        if _wait_until_ready(health_url, READY_TIMEOUT_SECONDS):
            full_url = dashboard_url + (f"?token={token}" if token else "")
            window.load_url(full_url)
        else:
            logger.error("native_window_ready_timeout", health_url=health_url)
            window.load_html(_UNREACHABLE_HTML)

    def _on_loaded() -> None:
        # Dispara só na primeira carga (a tela de loading) — chamadas
        # subsequentes de load_url/load_html não devem reiniciar o polling.
        window.events.loaded -= _on_loaded
        threading.Thread(target=_poll_and_load, daemon=True, name="native-ui-ready-poll").start()

    def _on_closing() -> None:
        """Best-effort: pede o shutdown gracioso do Gateway ao fechar a janela.

        Nunca lança — se o Gateway já caiu por outro motivo, não há nada a
        fazer aqui além de deixar a janela fechar normalmente.
        """
        try:
            request = urllib.request.Request(shutdown_url, method="POST")
            if token:
                request.add_header("Authorization", f"Bearer {token}")
            urllib.request.urlopen(request, timeout=SHUTDOWN_REQUEST_TIMEOUT_SECONDS)
        except Exception:
            logger.warning("native_window_shutdown_request_failed", shutdown_url=shutdown_url)

    window.events.loaded += _on_loaded
    window.events.closing += _on_closing

    webview.start()
