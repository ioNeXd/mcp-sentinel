"""Fixtures e helpers compartilhados dos testes."""

import contextlib
import socket
import sys
import time
from pathlib import Path
from typing import Any, Iterator

import structlog

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FAKE_BACKEND_PATH = ROOT / "tests" / "fake_backend.py"
FAKE_HTTP_BACKEND_PATH = ROOT / "tests" / "fake_backend_http.py"
FAKE_SSE_BACKEND_PATH = ROOT / "tests" / "fake_backend_sse.py"

from gateway.backend_manager import BackendManager  # noqa: E402
from gateway.clients.base import BaseClient  # noqa: E402
from gateway.config import BackendConfig, GatewayConfig  # noqa: E402
from gateway.errors import BackendError, BackendJsonRpcError  # noqa: E402
from gateway.registries import PromptRegistry, ResourceRegistry, ToolRegistry  # noqa: E402


def configure_quiet_structlog() -> None:
    """Configura structlog para os testes: processa eventos sem imprimir nada.

    O processor final é um no-op, então os módulos logam normalmente mas nada
    vaza para a saída do pytest. Testes que querem inspecionar logs chamam
    structlog.configure por conta própria (ex.: capturar eventos).
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _DiscardProcessor(),
        ],
        wrapper_class=structlog.BoundLogger,
        logger_factory=structlog.ReturnLoggerFactory(),
    )


class _DiscardProcessor:
    """Processor final que descarta o evento (nada é impresso)."""

    def __call__(self, _logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        return event_dict


configure_quiet_structlog()


async def _hang_forever() -> None:
    """Dorme indefinidamente (cancelável) — simula backend que não responde."""
    import asyncio

    await asyncio.Event().wait()


@contextlib.contextmanager
def capture_structlog_events() -> Iterator[list[dict[str, Any]]]:
    """Captura os eventos do structlog emitidos dentro do bloco ``with``.

    Reconfigura o structlog para anexar os eventos numa lista (incluindo os de
    nível debug) e restaura o baseline silencioso ao final. Usado nos testes
    que verificam QUAL evento foi emitido (ex.: regressões de logging).
    """
    events: list[dict[str, Any]] = []

    def capture(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        events.append(event_dict)
        return event_dict

    structlog.configure(
        processors=[structlog.contextvars.merge_contextvars, capture],
        wrapper_class=structlog.BoundLogger,
        logger_factory=structlog.ReturnLoggerFactory(),
    )
    try:
        yield events
    finally:
        configure_quiet_structlog()


# ----------------------------------------------------------------------
# Servidores fake como subprocesso (HTTP e SSE, Fase 3)
# ----------------------------------------------------------------------

PORT_RESERVATION_ATTEMPTS = 5
SERVER_READY_TIMEOUT_SECONDS = 15.0
SERVER_READY_POLL_SECONDS = 0.1


def free_port() -> int:
    """Pega uma porta livre do SO (o subprocesso do fake a usa em seguida)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def spawn_fake_server(script: Path, *args: str) -> tuple[int, Any]:
    """Sobe um fake backend HTTP/SSE como subprocesso numa porta livre.

    Devolve ``(port, process)`` — o chamador é responsável por encerrá-lo
    (os testes usam :func:`stop_fake_server`, que também aguarda a liberação
    para o teste seguinte reutilizar sem colisão). Detecta processo que morreu
    antes de abrir a porta (ex.: argumento inválido) e tenta nova porta.
    """
    import subprocess

    for _ in range(PORT_RESERVATION_ATTEMPTS):
        port = free_port()
        process = subprocess.Popen(
            [
                sys.executable,
                str(script),
                "--port",
                str(port),
                *args,
            ],
            cwd=str(ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if wait_for_port(port, process):
            return port, process
        exit_code = process.poll()
        stop_fake_server(process)
        if exit_code is not None:
            # Morreu sozinho antes de abrir a porta — sem o stderr aqui, o
            # sintoma seria só "não ficou pronto" após o timeout completo.
            raise RuntimeError(
                f"fake server {script.name} morreu no startup (exit {exit_code})"
            )
    raise RuntimeError(f"fake server {script.name} não ficou pronto")


def wait_for_port(port: int, process: Any) -> bool:
    """Aguarda a porta aceitar conexões, abortando se o processo morrer."""
    deadline = time.monotonic() + SERVER_READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False  # morreu antes de abrir a porta: não há o que esperar
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(SERVER_READY_POLL_SECONDS)
    return False


def stop_fake_server(process: Any) -> None:
    """Mata o subprocesso do fake e espera o processo de fato terminar."""
    process.kill()
    process.wait(timeout=10)


def spawn_fake_server_on_port(script: Path, port: int, *args: str) -> Any:
    """Sobe um fake backend numa porta FIXA (reocupar a porta de um que caiu)."""
    import subprocess

    process = subprocess.Popen(
        [sys.executable, str(script), "--port", str(port), *args],
        cwd=str(ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if not wait_for_port(port, process):
        exit_code = process.poll()
        stop_fake_server(process)
        raise RuntimeError(f"fake server {script.name} não subiu na porta {port}")
    return process


class FakeClient(BaseClient):
    """Client fake para testar o McpServer/BackendManager sem subprocessos.

    Responde tools/resources/prompts com listas fixas; qualquer outro método
    vira MethodNotFound (-32601), como um backend que não implementa aquele
    recurso. ``fail_calls`` derruba as chamadas de ``tools/call`` (BackendError);
    ``fail_method`` derruba chamadas de um método específico;
    ``hang_method`` faz chamadas de um método específico NUNCA responderem
    (exercita o caminho de timeout do manager/monitor);
    ``start_error`` faz o ``start()`` levantar BackendError (backend que não
    sobe — usado nos testes de restart/limite de tentativas).
    """

    def __init__(
        self,
        tools: list[dict[str, Any]] | None = None,
        resources: list[dict[str, Any]] | None = None,
        prompts: list[dict[str, Any]] | None = None,
        *,
        fail_calls: bool = False,
        fail_method: str | None = None,
        hang_method: str | None = None,
        start_error: bool = False,
    ) -> None:
        self.tools = tools if tools is not None else []
        self.resources = resources if resources is not None else []
        self.prompts = prompts if prompts is not None else []
        self.fail_calls = fail_calls
        self.fail_method = fail_method
        self.hang_method = hang_method
        self.start_error = start_error
        self.started = False
        self.stopped = False
        self.requests: list[tuple[str, dict[str, Any] | None]] = []

    async def start(self) -> None:
        if self.start_error:
            raise BackendError("backend fake não consegue subir")
        self.started = True
        self.stopped = False

    async def stop(self) -> None:
        self.stopped = True
        self.started = False

    def is_alive(self) -> bool:
        return self.started

    async def send_request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.requests.append((method, params))
        if self.hang_method is not None and method == self.hang_method:
            await _hang_forever()  # nunca responde: quem chama é que tem timeout
        if self.fail_method is not None and method == self.fail_method:
            raise BackendError(f"backend explodiu em {method}")
        if method == "ping":
            return {}
        if method == "tools/list":
            return {"tools": self.tools}
        if method == "tools/call":
            if self.fail_calls:
                raise BackendError("backend explodiu")
            name = params.get("name") if params else None
            return {
                "content": [{"type": "text", "text": f"resultado de {name}"}],
                "isError": False,
            }
        if method == "resources/list":
            return {"resources": self.resources}
        if method == "resources/read":
            uri = params.get("uri") if params else None
            return {
                "contents": [{"uri": uri, "mimeType": "text/plain", "text": f"conteúdo de {uri}"}]
            }
        if method == "prompts/list":
            return {"prompts": self.prompts}
        if method == "prompts/get":
            name = params.get("name") if params else None
            return {
                "description": f"prompt {name}",
                "messages": [{"role": "user", "content": {"type": "text", "text": f"olá {name}"}}],
            }
        raise BackendJsonRpcError(-32601, f"Method not found: {method}")


class FakeClientFactory:
    """Fábrica injetável de FakeClients usada pelo BackendManager nos testes.

    Guarda as instâncias criadas para que os testes possam matar um backend
    (``is_alive = False`` / ``fail_method``) e verificar o ciclo de restart.
    """

    def __init__(self, start_error: bool = False) -> None:
        self.start_error = start_error
        self.created: list[FakeClient] = []

    def __call__(self, backend_config: Any) -> FakeClient:
        client = FakeClient(
            tools=[{"name": "echo", "description": "fake", "inputSchema": {"type": "object"}}],
            start_error=self.start_error,
        )
        self.created.append(client)
        return client


def make_fake_manager(
    names: tuple[str, ...] = ("backend-a", "backend-b"),
    *,
    auto_restart: bool = True,
    max_restart_attempts: int = 5,
    factory: FakeClientFactory | None = None,
) -> tuple[BackendManager, FakeClientFactory]:
    """BackendManager configurado para teste, com fábrica de clients fake.

    O backoff fica zerado no manager devolvido (o teste que precisa exercitar
    tempos injeta seus próprios valores via monkeypatch em _backoff_seconds).
    """
    config = GatewayConfig(
        backends=[BackendConfig(name=name, command="fake", args=[]) for name in names],
        auto_restart=auto_restart,
        max_restart_attempts=max_restart_attempts,
        health_check_interval_seconds=3600.0,
    )
    manager = BackendManager(config, (ToolRegistry(), ResourceRegistry(), PromptRegistry()))
    used_factory = factory if factory is not None else FakeClientFactory()
    # Substitui a fábrica interna de clients (StdioClient) pela fake.
    manager._create_client = used_factory  # type: ignore[method-assign]
    return manager, used_factory


def make_manager_for_clients(
    clients: dict[str, FakeClient],
) -> tuple[BackendManager, tuple[ToolRegistry, ResourceRegistry, PromptRegistry]]:
    """BackendManager que devolve exatamente os clients informados (por nome).

    Usado pelos testes do McpServer, que constroem os FakeClients com
    ferramentas específicas e precisam que o manager os use tal quais.
    """
    config = GatewayConfig(
        backends=[BackendConfig(name=name, command="fake", args=[]) for name in clients],
        health_check_interval_seconds=3600.0,
    )
    registries = (ToolRegistry(), ResourceRegistry(), PromptRegistry())
    manager = BackendManager(config, registries)
    manager._create_client = lambda backend_config: clients[backend_config.name]  # type: ignore[method-assign,return-value,union-attr]
    return manager, registries
