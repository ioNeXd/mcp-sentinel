"""Testes do BackendManager (Fase 2): ciclo de vida, restart e backoff.

Os tempos de backoff são zerados via monkeypatch em ``_backoff_seconds`` —
a suíte não pode depender de sleeps reais. Como o restart roda em task
própria (o backoff não pode bloquear o monitor), os testes que precisam do
resultado da recuperação aguardam as tasks pendentes com ``drain_restarts``.
"""

import asyncio
import sys

import pytest

from conftest import FAKE_BACKEND_PATH, FakeClient, make_fake_manager
from gateway.backend_manager import BackendManager, BackendStatus
from gateway.config import BackendConfig, GatewayConfig
from gateway.errors import BackendError
from gateway.registries import PromptRegistry, ResourceRegistry, ToolRegistry

ECHO_TOOL = {
    "name": "echo",
    "description": "Repete texto.",
    "inputSchema": {"type": "object", "properties": {}},
}


def zero_backoff(manager: BackendManager) -> None:
    """Remove a espera do backoff nos testes (mantém a sequência testável à parte)."""
    manager._backoff_seconds = lambda consecutive_failures: 0.0  # type: ignore[method-assign]


async def drain_restarts(manager: BackendManager) -> None:
    """Aguarda as tasks de restart pendentes terminarem."""
    pending = [t for t in manager._restart_tasks.values() if not t.done()]
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.asyncio
async def test_start_all_registra_backends_nos_registries() -> None:
    manager, factory = make_fake_manager(("backend-a", "backend-b"))
    await manager.start_all()
    try:
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        assert manager.status_of("backend-b") is BackendStatus.RUNNING
        # Cada backend registrou sua tool namespaced nos registries.
        tool_names = {e.namespaced for e in manager.registries[0].list_all()}
        assert tool_names == {"backend-a.echo", "backend-b.echo"}
        # Clients criados e iniciados pela fábrica.
        assert len(factory.created) == 2
        assert all(c.started for c in factory.created)
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_stop_all_limpa_registries_e_para_clients() -> None:
    manager, factory = make_fake_manager(("backend-a", "backend-b"))
    await manager.start_all()
    await manager.stop_all()
    assert manager.registries[0].list_all() == []
    assert manager.registries[1].list_all() == []
    assert manager.registries[2].list_all() == []
    assert all(c.stopped for c in factory.created)
    assert manager.status_of("backend-a") is BackendStatus.OFFLINE
    await manager.stop_all()  # idempotente


@pytest.mark.asyncio
async def test_start_all_um_backend_falhando_nao_derruba_os_demais() -> None:
    manager, _ = make_fake_manager(("bom", "ruim"))
    original_create = manager._create_client

    def factory(backend_config: object) -> FakeClient:
        if backend_config.name == "ruim":  # type: ignore[attr-defined]
            return FakeClient(start_error=True)
        return original_create(backend_config)

    manager._create_client = factory  # type: ignore[method-assign]
    await manager.start_all()  # não levanta: só um falhou
    assert manager.status_of("bom") is BackendStatus.RUNNING
    assert manager.status_of("ruim") is BackendStatus.OFFLINE
    await manager.stop_all()


@pytest.mark.asyncio
async def test_start_all_todos_falhando_levanta_erro() -> None:
    manager, _ = make_fake_manager(("a", "b"))
    # Factory custom: todos os clients falham ao subir.
    manager._create_client = lambda backend_config: FakeClient(start_error=True)  # type: ignore[method-assign,return-value]
    with pytest.raises(BackendError, match="nenhum backend"):
        await manager.start_all()


@pytest.mark.asyncio
async def test_get_client_levanta_para_backend_offline() -> None:
    manager, _ = make_fake_manager(("backend-a",))
    with pytest.raises(BackendError, match="não existe"):
        manager.get_client("fantasma")
    await manager.start_all()
    try:
        assert manager.get_client("backend-a") is manager.get_state("backend-a").client
        manager.get_state("backend-a").status = BackendStatus.OFFLINE
        with pytest.raises(BackendError, match="não está disponível"):
            manager.get_client("backend-a")
    finally:
        await manager.stop_all()


# ----------------------------------------------------------------------
# Detecção de queda (health check)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_processo_morto_detectado_como_offline_e_registros_removidos() -> None:
    manager, factory = make_fake_manager(("backend-a",))
    await manager.start_all()
    try:
        client = factory.created[0]
        await client.stop()  # simula o processo morrendo
        status = await manager.check_and_recover("backend-a")
        assert status is BackendStatus.OFFLINE
        assert manager.registries[0].list_all() == []  # tools removidas
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_backend_vivo_mas_sem_responder_ping_vira_offline() -> None:
    manager, factory = make_fake_manager(("backend-a",))
    await manager.start_all()
    try:
        factory.created[0].fail_method = "ping"  # vivo, mas não responde (timeout)
        status = await manager.check_and_recover("backend-a")
        assert status is BackendStatus.OFFLINE
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_backend_que_nao_implementa_ping_continua_running() -> None:
    """Erro JSON-RPC ao ping prova que o peer está vivo — não conta como offline.

    Regressão do smoke test da Fase 2: o fake antigo respondia -32603 ao ping e
    o health check derrubava backends perfeitamente saudáveis.
    """
    manager, factory = make_fake_manager(("backend-a",))
    zero_backoff(manager)
    await manager.start_all()
    try:

        async def method_not_found_ping(method: str, params: object = None) -> object:
            from gateway.errors import BackendJsonRpcError as BJRE

            if method == "ping":
                raise BJRE(-32601, "Method not found: ping")
            return await FakeClient.send_request(factory.created[0], method, params)  # type: ignore[arg-type]

        factory.created[0].send_request = method_not_found_ping  # type: ignore[method-assign]
        status = await manager.check_and_recover("backend-a")
        assert status is BackendStatus.RUNNING
        assert len(factory.created) == 1  # nenhum restart foi acionado
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_recuperacao_observavel_quando_volta_a_responder() -> None:
    manager, factory = make_fake_manager(("backend-a",))
    zero_backoff(manager)
    await manager.start_all()
    try:
        client = factory.created[0]
        client.fail_method = "ping"
        await manager.check_and_recover("backend-a")  # offline + restart agendado
        await drain_restarts(manager)
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        # O client antigo falha ping, mas o restart criou um novo client.
        assert factory.created[0] is not factory.created[-1]
        # Novo ciclo: saudável, e o contador zera.
        state = manager.get_state("backend-a")
        state.consecutive_failures = 1  # como se tivesse ficado degradado
        status = await manager.check_and_recover("backend-a")
        assert status is BackendStatus.RUNNING
        assert state.consecutive_failures == 0
    finally:
        await manager.stop_all()


# ----------------------------------------------------------------------
# Auto-restart com backoff
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restart_registra_tools_novas_sem_duplicar() -> None:
    manager, factory = make_fake_manager(("backend-a",))
    zero_backoff(manager)
    await manager.start_all()
    try:
        await factory.created[0].stop()  # processo morre
        await manager.check_and_recover("backend-a")  # offline + restart agendado
        await drain_restarts(manager)
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        # Tools re-registradas exatamente uma vez (sem duplicatas/obsoletas).
        entries = manager.registries[0].list_all()
        assert [e.namespaced for e in entries] == ["backend-a.echo"]
        assert len(factory.created) == 2  # client antigo + client novo
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_falhas_repetidas_levam_ao_estado_failed_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """2.11 — max_restart_attempts = NÚMERO DE TENTATIVAS de restart.

    Semântica documentada no README: com N, o Gateway tenta reiniciar EXATAMENTE
    N vezes (contando as chamadas a client.start() dos restarts); só então o
    backend vira 'failed' (terminal) e o monitor para de tentar.
    """
    manager, _ = make_fake_manager(("backend-a",), max_restart_attempts=3)
    zero_backoff(manager)
    start_calls: list[int] = []

    def flaky_factory(backend_config: object) -> FakeClient:
        # Sobe só na primeira vez (start_all); todos os restarts falham.
        start_calls.append(1)
        return FakeClient(tools=[ECHO_TOOL], start_error=len(start_calls) > 1)

    manager._create_client = flaky_factory  # type: ignore[method-assign]
    await manager.start_all()
    try:
        state = manager.get_state("backend-a")
        assert state.client is not None
        assert len(start_calls) == 1  # startup
        await state.client.stop()  # derruba sem restart automático
        state.status = BackendStatus.OFFLINE
        state.client = None
        manager._unregister("backend-a")
        # A detecção de queda (ramo `was_running` do check_and_recover)
        # incrementaria o contador aqui (0 -> 1): o teardown manual pula esse
        # ramo, então a simulação aplica o delta explicitamente.
        state.consecutive_failures += 1

        # Exatamente N tentativas de restart (chamadas a start() além do
        # startup) acontecem antes do estado terminal.
        for attempt in range(1, 4):  # tentativas 1, 2 e 3 (N = 3)
            await manager.check_and_recover("backend-a")
            await drain_restarts(manager)
            assert manager.status_of("backend-a") is BackendStatus.OFFLINE
            assert len(start_calls) == 1 + attempt
        assert state.consecutive_failures == 4  # queda + 3 restarts falhos

        # Checagem seguinte: limite esgotado -> failed (terminal), sem nova
        # tentativa de restart.
        await manager.check_and_recover("backend-a")
        await drain_restarts(manager)
        assert manager.status_of("backend-a") is BackendStatus.FAILED
        assert len(start_calls) == 4  # nenhuma tentativa a mais

        # Ciclos seguintes NÃO tentam mais restart (nenhum client novo criado).
        for _ in range(3):
            await manager.check_and_recover("backend-a")
            await drain_restarts(manager)
        assert len(start_calls) == 4
        assert manager.status_of("backend-a") is BackendStatus.FAILED
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_auto_restart_desligado_mantem_offline() -> None:
    manager, factory = make_fake_manager(("backend-a",), auto_restart=False)
    await manager.start_all()
    try:
        await factory.created[0].stop()
        status = await manager.check_and_recover("backend-a")
        assert status is BackendStatus.OFFLINE
        assert manager.status_of("backend-a") is BackendStatus.OFFLINE
    finally:
        await manager.stop_all()


def test_sequencia_de_backoff_exponencial() -> None:
    manager, _ = make_fake_manager(("backend-a",))
    expected = [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]
    actual = [manager._backoff_seconds(f) for f in range(1, 8)]
    assert actual == expected
    assert manager._backoff_seconds(0) == 1.0


@pytest.mark.asyncio
async def test_restart_manual_recupera_backend() -> None:
    manager, factory = make_fake_manager(("backend-a",))
    zero_backoff(manager)
    await manager.start_all()
    try:
        await manager.restart("backend-a")
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        assert [e.namespaced for e in manager.registries[0].list_all()] == ["backend-a.echo"]
        with pytest.raises(BackendError, match="não existe"):
            await manager.restart("fantasma")
    finally:
        await manager.stop_all()


# ----------------------------------------------------------------------
# Resumos de observabilidade
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_summary_reflete_estado() -> None:
    """Com tudo em pé o resumo é 'ok'; derrubando um backend vira 'degraded'."""
    manager, factory = make_fake_manager(("backend-a", "backend-b"))
    zero_backoff(manager)
    await manager.start_all()
    try:
        summary = manager.health_summary()
        assert summary["status"] == "ok"
        assert summary["backends"]["running"] == 2
        assert summary["tools_count"] == 2

        await factory.created[0].stop()  # backend-a morre
        await manager.check_and_recover("backend-a")  # offline -> restart agendado
        await drain_restarts(manager)
        # Após a recuperação, backend-a voltou a running com client novo.
        assert manager.health_summary()["backends"]["running"] == 2

        # Agora um backend que não volta (sem auto-restart) -> degraded.
        manager.config.auto_restart = False
        state_a = manager.get_state("backend-a")
        assert state_a.client is not None
        await state_a.client.stop()
        await manager.check_and_recover("backend-a")
        summary = manager.health_summary()
        assert summary["status"] == "degraded"
        assert summary["backends"]["offline"] == 1
    finally:
        await manager.stop_all()


# ----------------------------------------------------------------------
# Graceful shutdown: processos filhos reais não ficam órfãos
# ----------------------------------------------------------------------


def make_real_manager(names: tuple[str, ...]) -> BackendManager:
    """BackendManager com fábrica padrão (StdioClient real, subprocessos)."""
    config = GatewayConfig(
        backends=[
            BackendConfig(name=name, command=sys.executable, args=[str(FAKE_BACKEND_PATH)])
            for name in names
        ],
        health_check_interval_seconds=3600.0,
    )
    return BackendManager(config, (ToolRegistry(), ResourceRegistry(), PromptRegistry()))


@pytest.mark.asyncio
async def test_stop_all_encerra_processos_filhos_sem_deixar_orfaos() -> None:
    manager = make_real_manager(("backend-a", "backend-b"))
    await manager.start_all()
    processes = []
    for state in manager.all_states().values():
        assert state.client is not None
        process = state.client._process  # noqa: SLF001 — inspeção em teste
        assert process is not None and process.returncode is None
        processes.append(process)
    await manager.stop_all()
    for process in processes:
        assert process.returncode is not None  # processo realmente terminou


# ----------------------------------------------------------------------
# Fase 3: HttpClient/SseClient — fábrica por tipo, health e restart
# ----------------------------------------------------------------------


@pytest.fixture
def http_fake():
    """Fake HTTP num subprocesso; devolve a url base."""
    from conftest import FAKE_HTTP_BACKEND_PATH, spawn_fake_server, stop_fake_server

    port, process = spawn_fake_server(FAKE_HTTP_BACKEND_PATH)
    yield f"http://127.0.0.1:{port}"
    stop_fake_server(process)


@pytest.fixture
def sse_fake():
    """Fake SSE num subprocesso; devolve a url base."""
    from conftest import FAKE_SSE_BACKEND_PATH, spawn_fake_server, stop_fake_server

    port, process = spawn_fake_server(FAKE_SSE_BACKEND_PATH)
    yield f"http://127.0.0.1:{port}"
    stop_fake_server(process)


def make_mixed_manager(
    http_url: str,
    sse_url: str,
    **config_kwargs: object,
) -> BackendManager:
    """Manager com os 3 tipos simultâneos usando a FÁBRICA REAL (sem override).

    O stdio usa o fake_backend.py; http/sse apontam para os fakes já no ar.
    """
    config = GatewayConfig(
        backends=[
            BackendConfig(
                name="local", command=sys.executable, args=[str(FAKE_BACKEND_PATH)]
            ),
            BackendConfig(name="remoto", type="http", url=http_url),
            BackendConfig(name="eventos", type="sse", url=sse_url),
        ],
        health_check_interval_seconds=3600.0,
        **config_kwargs,  # type: ignore[arg-type]
    )
    return BackendManager(config, (ToolRegistry(), ResourceRegistry(), PromptRegistry()))


@pytest.mark.asyncio
async def test_start_all_com_os_tres_tipos_agrega_e_roteia(
    http_fake: str, sse_fake: str
) -> None:
    """Critério de aceite da Fase 3: stdio + http + sse agregados e roteados."""
    from gateway.server import McpServer

    manager = make_mixed_manager(http_fake, sse_fake)
    server = McpServer(manager, manager.registries)
    await server.start()
    try:
        for name in ("local", "remoto", "eventos"):
            assert manager.status_of(name) is BackendStatus.RUNNING
            # A fábrica real escolheu o transporte certo para cada tipo.
            client = manager.get_client(name)
            assert type(client).__name__ in {"StdioClient", "HttpClient", "SseClient"}

        # tools/list agrega os TRÊS backends (cada fake expõe echo+add).
        response = await server.process_message(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
        )
        assert response is not None and response.get("error") is None
        names = {tool["name"] for tool in response["result"]["tools"]}
        assert names == {
            "local.echo",
            "local.add",
            "remoto.echo",
            "remoto.add",
            "eventos.echo",
            "eventos.add",
        }

        # tools/call roteia para o backend certo (texto devolvido por cada um).
        for backend in ("local", "remoto", "eventos"):
            response = await server.process_message(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": f"{backend}.echo",
                        "arguments": {"text": f"oi-{backend}"},
                    },
                }
            )
            assert response is not None and response.get("error") is None, response
            assert response["result"]["content"][0]["text"] == f"oi-{backend}"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_backend_http_caido_no_start_nao_derruba_os_demais(
    sse_fake: str,
) -> None:
    """HTTP morto no startup: fica offline; stdio/sse sobem normalmente."""
    manager = make_mixed_manager("http://127.0.0.1:9", sse_fake)  # porta vazia
    await manager.start_all()  # não levanta: só o http falhou
    try:
        assert manager.status_of("local") is BackendStatus.RUNNING
        assert manager.status_of("remoto") is BackendStatus.OFFLINE
        assert manager.status_of("eventos") is BackendStatus.RUNNING
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_health_e_restart_http_reconecta_quando_servidor_volta() -> None:
    """Ciclo completo do http: cai → offline → servidor volta → restart reconecta.

    Diferença do stdio: o Gateway não é pai do processo — o restart NÃO cria
    processo nenhum, só reconecta na url quando o backend remoto voltou. É por
    isso que o teste reocupa a MESMA porta (o config não muda).
    """
    from conftest import (
        FAKE_HTTP_BACKEND_PATH,
        free_port,
        spawn_fake_server_on_port,
        stop_fake_server,
    )

    port = free_port()
    url = f"http://127.0.0.1:{port}"
    server_process = spawn_fake_server_on_port(FAKE_HTTP_BACKEND_PATH, port)
    manager = make_mixed_manager(url, url)  # sse aponta pro mesmo lugar, não é exercitado
    manager._backoff_seconds = lambda failures: 0.0  # type: ignore[method-assign]
    await manager.start_all()
    try:
        assert manager.status_of("remoto") is BackendStatus.RUNNING

        # O backend remoto "cai" (o processo do fake é morto).
        stop_fake_server(server_process)
        status = await manager.check_and_recover("remoto")
        assert status is BackendStatus.OFFLINE
        # Tools do remoto saem dos registries (as do stdio "local" permanecem).
        assert [e for e in manager.registries[0].list_all() if e.backend == "remoto"] == []

        # O servidor remoto volta ao ar na MESMA porta (config não mudou).
        respawned = spawn_fake_server_on_port(FAKE_HTTP_BACKEND_PATH, port)
        try:
            await manager.check_and_recover("remoto")
            await drain_restarts(manager)
            assert manager.status_of("remoto") is BackendStatus.RUNNING
            entries = [e for e in manager.registries[0].list_all() if e.backend == "remoto"]
            assert [e.namespaced for e in entries] == ["remoto.echo", "remoto.add"]
            # tools/call funciona no client reconectado.
            response = await server_call(manager, "remoto.echo", {"text": "voltou"})
            assert response["result"]["content"][0]["text"] == "voltou"
        finally:
            stop_fake_server(respawned)
    finally:
        await manager.stop_all()


async def server_call(
    manager: BackendManager, tool_name: str, arguments: dict[str, object]
) -> dict[str, object]:
    """tools/call via McpServer do manager (helper dos testes de integração)."""
    from gateway.server import McpServer

    server = McpServer(manager, manager.registries)
    response = await server.process_message(
        {
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }
    )
    assert response is not None
    return response


@pytest.mark.asyncio
async def test_health_e_restart_sse_reconecta_quando_servidor_volta() -> None:
    """Mesmo ciclo para o SSE: stream cai → offline → servidor volta → reconecta."""
    from conftest import (
        FAKE_SSE_BACKEND_PATH,
        free_port,
        spawn_fake_server_on_port,
        stop_fake_server,
    )

    port = free_port()
    url = f"http://127.0.0.1:{port}"
    server_process = spawn_fake_server_on_port(FAKE_SSE_BACKEND_PATH, port)
    manager = make_mixed_manager(url, url)
    manager._backoff_seconds = lambda failures: 0.0  # type: ignore[method-assign]
    await manager.start_all()
    try:
        assert manager.status_of("eventos") is BackendStatus.RUNNING

        stop_fake_server(server_process)
        status = await manager.check_and_recover("eventos")
        assert status is BackendStatus.OFFLINE

        respawned = spawn_fake_server_on_port(FAKE_SSE_BACKEND_PATH, port)
        try:
            await manager.check_and_recover("eventos")
            await drain_restarts(manager)
            assert manager.status_of("eventos") is BackendStatus.RUNNING
            response = await server_call(manager, "eventos.echo", {"text": "de-volta"})
            assert response["result"]["content"][0]["text"] == "de-volta"
        finally:
            stop_fake_server(respawned)
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_health_monitor_funciona_para_os_tres_tipos(
    http_fake: str, sse_fake: str
) -> None:
    """O ciclo do HealthMonitor (Fase 2) se aplica aos 3 transportes."""
    from conftest import FAKE_SSE_BACKEND_PATH, spawn_fake_server_on_port, stop_fake_server
    from gateway.health_monitor import HealthMonitor

    port = int(sse_fake.rsplit(":", 1)[1])
    manager = make_mixed_manager(http_fake, sse_fake)
    manager._backoff_seconds = lambda failures: 0.0  # type: ignore[method-assign]
    monitor = HealthMonitor(manager, interval_seconds=0.05)
    await manager.start_all()
    try:
        # Fase 1 do teste: DETECÇÃO da queda, sem auto-restart (o servidor
        # remoto está no ar; com restart ligado a recuperação seria instantânea
        # e o estado OFFLINE nunca seria observável).
        manager.config.auto_restart = False
        monitor.start()

        # O stream SSE "cai" (peer some na perspectiva do Gateway): o monitor
        # detecta via is_alive() e marca offline, como num stdio morto.
        state_sse = manager.get_state("eventos")
        assert state_sse.client is not None
        await state_sse.client.stop()
        for _ in range(20):
            if manager.status_of("eventos") is BackendStatus.OFFLINE:
                break
            await asyncio.sleep(0.03)
        assert manager.status_of("eventos") is BackendStatus.OFFLINE

        # Fase 2: o servidor remoto volta na mesma porta, o restart é reativado
        # e o monitor recupera o backend sozinho.
        manager.config.auto_restart = True
        respawned = spawn_fake_server_on_port(FAKE_SSE_BACKEND_PATH, port)
        try:
            for _ in range(40):
                if manager.status_of("eventos") is BackendStatus.RUNNING:
                    break
                await asyncio.sleep(0.05)
            assert manager.status_of("eventos") is BackendStatus.RUNNING
            # Os outros dois (stdio e http) nunca saíram de running.
            assert manager.status_of("local") is BackendStatus.RUNNING
            assert manager.status_of("remoto") is BackendStatus.RUNNING
        finally:
            stop_fake_server(respawned)
    finally:
        await monitor.stop()
        await manager.stop_all()


# ----------------------------------------------------------------------
# Robustez do ciclo de vida (registro atômico, teardown resiliente, corridas)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_falho_no_start_nao_deixa_registro_nem_client_orfao() -> None:
    """2.1 — falha no registro (item inválido): cleanup completo e erro claro.

    O client sobe mas devolve uma tool com nome contendo '.' (rejeitada pelo
    registry, Bloco 1): _start_one desfaz registros, para o client e propaga —
    nenhum registry com entrada órfã, nenhuma conexão órfã, estado OFFLINE.
    """
    manager, _ = make_fake_manager(("backend-a",))
    TOOL_INVALIDA = {
        "name": "echo.v2",  # contém '.': rejeitada pelo ToolRegistry (1.3)
        "description": "inválida",
        "inputSchema": {"type": "object"},
    }

    def factory(backend_config: object) -> FakeClient:
        client = FakeClient(tools=[TOOL_INVALIDA])
        calls: list[bool] = []

        async def tracked_stop() -> None:
            calls.append(True)
            await FakeClient.stop(client)  # type: ignore[arg-type]

        client.stop = tracked_stop  # type: ignore[method-assign]
        return client

    manager._create_client = factory  # type: ignore[method-assign]
    with pytest.raises(BackendError):
        await manager._start_one("backend-a")
    # Nenhum registro órfão em NENHUM dos três registries.
    for registry in manager.registries:
        assert registry.list_all() == []
    state = manager.get_state("backend-a")
    assert state is not None
    assert state.client is None  # nunca ficou apontando para client órfão


@pytest.mark.asyncio
async def test_stop_all_continua_mesmo_com_client_que_falha_ao_parar() -> None:
    """2.2 — stop() que estoura não impede o cleanup dos demais backends."""
    manager, factory = make_fake_manager(("a", "b", "c"))
    await manager.start_all()

    async def broken_stop() -> None:
        raise RuntimeError("falha simulada no stop")

    factory.created[1].stop = broken_stop  # type: ignore[method-assign]
    await manager.stop_all()  # não levanta
    # Todos os backends passaram pelo cleanup completo (inclusive o "b").
    for name in ("a", "b", "c"):
        state = manager.get_state(name)
        assert state is not None
        assert state.client is None
        assert state.status is BackendStatus.OFFLINE
    for registry in manager.registries:
        assert registry.list_all() == []


@pytest.mark.asyncio
async def test_disable_e_restart_concorrentes_resultam_em_disabled() -> None:
    """2.7 — corrida real disable vs. restart manual: o final é sempre DISABLED.

    O client fake tem start() com delay controlado: o restart entra no lock e
    fica em _start_one; o disable chega DEPOIS, espera o lock (não sobrescreve
    nada no meio) e ao final o estado é DISABLED, nunca RUNNING.
    """
    manager, _ = make_fake_manager(("backend-a",))
    zero_backoff(manager)
    await manager.start_all()
    try:
        original_start = FakeClient.start
        release = asyncio.Event()
        start_entered = asyncio.Event()

        async def slow_start(self: FakeClient) -> None:
            start_entered.set()
            await release.wait()
            await original_start(self)

        # O delay precisa estar no client que o restart() vai CRIAR dentro de
        # _start_one (o client atual, `created[0]`, é derrubado pelo teardown
        # antes) — senão o start_entered nunca dispara.
        def slow_factory(backend_config: object) -> FakeClient:
            client = FakeClient()
            client.start = lambda: slow_start(client)  # type: ignore[method-assign]
            return client

        manager._create_client = slow_factory  # type: ignore[method-assign]

        restart_task = asyncio.create_task(manager.restart("backend-a"))
        await start_entered.wait()  # restart está dentro do lock, em _start_one
        disable_task = asyncio.create_task(manager.disable("backend-a"))
        # Dá chance de o disable rodar até o ponto do lock.
        await asyncio.sleep(0.05)
        release.set()
        await asyncio.gather(restart_task, disable_task)
        assert manager.status_of("backend-a") is BackendStatus.DISABLED
        assert manager.get_state("backend-a").client is None
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_restart_task_remove_a_propria_entrada_ao_terminar() -> None:
    """2.8 — task de restart concluída sai de _restart_tasks (sem entrada morta)."""
    manager, factory = make_fake_manager(("backend-a",))
    zero_backoff(manager)
    await manager.start_all()
    try:
        await factory.created[0].stop()
        await manager.check_and_recover("backend-a")  # restart agendado
        await drain_restarts(manager)
        assert manager.status_of("backend-a") is BackendStatus.RUNNING
        assert "backend-a" not in manager._restart_tasks
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_restart_manual_http_reconecta(http_fake: str) -> None:
    """Restart manual com o client fechado (servidor no ar): reconecta e re-registra."""
    manager = make_mixed_manager(http_fake, http_fake)
    manager._backoff_seconds = lambda failures: 0.0  # type: ignore[method-assign]
    await manager.start_all()
    try:
        state = manager.get_state("remoto")
        assert state.client is not None
        await state.client.stop()
        await manager.restart("remoto")
        assert manager.status_of("remoto") is BackendStatus.RUNNING
        response = await server_call(manager, "remoto.echo", {"text": "manual"})
        assert response["result"]["content"][0]["text"] == "manual"
    finally:
        await manager.stop_all()
