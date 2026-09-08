"""Regressões do ciclo de vida do stream SSE (queda, corrida com stop, órfãs).

Cobre dois bugs reportados em uso real:
1. ``sse_stream_perdido`` era engolido quando a queda do stream corria em
   paralelo a um ``stop()`` (ex.: health monitor encerrando o client) — só
   ``backend_detected_offline`` aparecia, sem o motivo;
2. futures órfãs (request abandonada por wait_for externo, depois derrubada
   pela queda do stream) recebiam ``set_exception`` sem consumidor e o GC do
   asyncio logava "Future exception was never retrieved" fora do structlog.
"""

import asyncio

import pytest

from conftest import (
    FAKE_SSE_BACKEND_PATH,
    capture_structlog_events,
    spawn_fake_server,
    stop_fake_server,
)
from gateway.clients.sse_client import SseClient
from gateway.config import BackendConfig

READY_TIMEOUT = 10.0


def make_client(url: str, timeout: float = READY_TIMEOUT) -> SseClient:
    config = BackendConfig(name="sse-regressao", type="sse", url=url)
    return SseClient(config, request_timeout=timeout)


@pytest.fixture
def sse_backend():
    port, process = spawn_fake_server(FAKE_SSE_BACKEND_PATH, "--delay", "2")
    yield f"http://127.0.0.1:{port}", process
    stop_fake_server(process)


@pytest.mark.asyncio
async def test_log_sse_stream_perdido_emitido_na_queda_real(sse_backend) -> None:
    """O log específico da queda do stream aparece (não só backend_detected_offline)."""
    url, process = sse_backend
    client = make_client(url)
    await client.start()
    try:
        with capture_structlog_events() as events:
            stop_fake_server(process)
            await asyncio.sleep(0.5)  # leitor detecta a queda (ReadError) e loga
        logged = [e["event"] for e in events]
        assert "sse_stream_perdido" in logged
        event = next(e for e in events if e["event"] == "sse_stream_perdido")
        assert event["backend"] == "sse-regressao"
        assert event["reason"]  # motivo da queda presente no log
        assert not client.is_alive()
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_log_sse_stream_perdido_mesmo_com_stop_em_andamento(sse_backend) -> None:
    """Queda observada pelo leitor + stop() em seguida: o log não é engolido.

    Ordem realista do health monitor/shutdown: o leitor detecta a queda do
    stream e emite o log; o ``stop()`` (do monitor ou do shutdown) chega em
    seguida. A regressão garantia que o guard ``_stopped`` NÃO venha antes do
    log e o engula. (Se o ``stop()`` cancelar o leitor antes de ele observar a
    queda, não há log — o fechamento foi deliberado, não uma queda.)
    """
    url, process = sse_backend
    client = make_client(url)
    await client.start()
    with capture_structlog_events() as events:
        stop_fake_server(process)  # backend remoto morre
        await asyncio.sleep(0.3)  # leitor observa a queda e emite o log
        await client.stop()
    assert "sse_stream_perdido" in [e["event"] for e in events]


@pytest.mark.asyncio
async def test_log_emitido_mesmo_quando_cliente_ja_esta_stopped() -> None:
    """Unidade do guard: ``_on_stream_closed`` loga ANTES de respeitar _stopped.

    Determinístico (sem corrida de processos): cliente marcado como stopped e
    a notificação de queda chegando depois — o evento precisa existir.
    """
    config = BackendConfig(name="sse-guard", type="sse", url="http://127.0.0.1:9")
    client = SseClient(config, request_timeout=READY_TIMEOUT)
    client._ready.set()  # stream "já esteve pronto"
    client._stopped = True
    with capture_structlog_events() as events:
        await client._on_stream_closed("stream encerrado pelo backend")  # noqa: SLF001
    assert "sse_stream_perdido" in [e["event"] for e in events]


@pytest.mark.asyncio
async def test_future_orfa_da_queda_nao_loga_exception_never_retrieved(sse_backend) -> None:
    """Request abandonada por wait_for + stream caindo: nada vaza no asyncio.

    Reproduz o cenário real: o health check (wait_for de 2s) desiste do ping;
    o stream cai em seguida e derruba a future que ficou órfã em ``_pending``.
    O aviso "Future exception was never retrieved" é emitido pelo GC do
    asyncio de forma assíncrona — capturamos o handler de exceção do loop por
    uma janela generosa para observá-lo (ou constatar a ausência).
    """
    url, process = sse_backend
    client = make_client(url)
    await client.start()
    try:
        abandoned: list[asyncio.Future] = []
        original_create = asyncio.get_running_loop().create_future

        def spy_create_future() -> asyncio.Future:
            future = original_create()
            abandoned.append(future)
            return future

        asyncio.get_running_loop().create_future = spy_create_future  # type: ignore[method-assign]
        try:
            # wait_for externo desiste (a future da request fica órfã em _pending).
            # tools/list num servidor com --delay: a resposta não chega no prazo.
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(client.send_request("tools/list"), timeout=0.15)
        finally:
            asyncio.get_running_loop().create_future = original_create  # type: ignore[method-assign]

        orphan = next(f for f in abandoned if not f.done())  # pendente e sem dono
        assert orphan in client._pending.values()  # noqa: SLF001 — é o leak do cenário real

        # O stream cai e derruba a future órfã com set_exception (guardado).
        with capture_structlog_events() as events:
            stop_fake_server(process)
            await asyncio.sleep(0.5)

        assert "sse_stream_perdido" in [e["event"] for e in events]
        assert orphan.done() and not orphan.cancelled()
        assert orphan.exception() is not None  # noqa: SLF001 — corpo do teste

        # Janela generosa: o GC do asyncio reporta futures não consumidas
        # de forma assíncrona; o done_callback do set_exception_guarded já
        # recuperou a exceção, então NADA deve aparecer aqui.
        emitted: list[BaseException] = []
        loop = asyncio.get_running_loop()
        original_handler = loop.get_exception_handler()

        def spy_handler(loop: object, context: dict) -> None:
            if "exception" in context:
                emitted.append(context["exception"])

        loop.set_exception_handler(spy_handler)
        try:
            import gc

            for _ in range(6):
                await asyncio.sleep(0.05)
                gc.collect()
        finally:
            loop.set_exception_handler(original_handler)
        never_retrieved = [e for e in emitted if "Future exception was never retrieved" in str(e)]
        assert not never_retrieved, f"exceção órfã vazou: {never_retrieved}"
    finally:
        await client.stop()
        stop_fake_server(process)


@pytest.mark.asyncio
async def test_set_exception_guarded_consumindo_excecao_descartada() -> None:
    """O callback do helper recupera a exceção e emite o debug no structlog."""
    from gateway.clients.base import set_exception_guarded
    from gateway.errors import BackendDisconnectedError

    with capture_structlog_events() as events:
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        set_exception_guarded(
            future, BackendDisconnectedError("ninguém vai me consumir"), backend="b1"
        )
        await asyncio.sleep(0)  # done_callbacks rodam no próximo passo do loop
        assert future.exception() is not None  # recuperação direta também funciona

    discarded = [e for e in events if e["event"] == "future_exception_descartada"]
    assert discarded and discarded[0]["backend"] == "b1"
