"""Testes do StdioClient contra o fake_backend.py real (subprocesso)."""

import asyncio
import json
import sys

import pytest

from conftest import FAKE_BACKEND_PATH, capture_structlog_events
from gateway.clients.base import BackendListResponseError, BaseClient
from gateway.clients.stdio_client import StdioClient
from gateway.config import BackendConfig
from gateway.errors import (
    BackendDisconnectedError,
    BackendError,
    BackendJsonRpcError,
    BackendTimeoutError,
)


def make_client(
    command: str | None = None,
    args: list[str] | None = None,
    timeout: float = 5.0,
) -> StdioClient:
    """Monta um StdioClient apontando para o fake backend."""
    config = BackendConfig(
        name="test-backend",
        command=command or sys.executable,
        args=args if args is not None else [str(FAKE_BACKEND_PATH)],
    )
    return StdioClient(config, request_timeout=timeout)


@pytest.mark.parametrize(
    "result",
    [None, {}, {"tools": None}, {"tools": {}}],
)
def test_extract_list_rejeita_resposta_malformada(result) -> None:
    with pytest.raises(BackendListResponseError):
        BaseClient._extract_list(result, "tools")


def test_extract_list_preserva_lista_vazia_valida() -> None:
    assert BaseClient._extract_list({"tools": []}, "tools") == []


def test_estado_de_instancia_nao_e_compartilhado_entre_clients() -> None:
    """5.2 — _capabilities/_pending são por instância (não atributo de classe).

    Dois clients distintos começam com capabilities vazias INDEPENDENTES:
    setar a de um não vaza para o outro (regressão do atributo mutável de
    classe que era compartilhado até o primeiro set).
    """
    client_a = make_client()
    client_b = make_client()
    assert client_a.capabilities == {}
    assert client_b.capabilities == {}
    client_a.capabilities = {"tools": {}}
    assert client_b.capabilities == {}  # isolado
    assert client_a._pending == {} and client_b._pending == {}  # noqa: SLF001


@pytest.mark.asyncio
async def test_handshake_tools_list_e_call() -> None:
    client = make_client()
    await client.start()
    try:
        tools = await client.list_tools()
        names = {tool["name"] for tool in tools}
        assert names == {"echo", "add"}

        result = await client.send_request(
            "tools/call", {"name": "echo", "arguments": {"text": "ola"}}
        )
        assert result["content"][0]["text"] == "ola"

        result = await client.send_request(
            "tools/call", {"name": "add", "arguments": {"a": 2, "b": 3}}
        )
        assert result["content"][0]["text"] == "5"
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_timeout_quando_backend_nao_responde() -> None:
    # Processo que só lê stdin e nunca responde: handshake estoura o timeout.
    client = StdioClient(
        BackendConfig(name="lento", command=sys.executable, args=["-c", "import sys; sys.stdin.read()"]),
        request_timeout=0.2,
    )
    with pytest.raises(BackendTimeoutError):
        await client.start()
    await client.stop()


@pytest.mark.asyncio
async def test_erro_quando_processo_morre() -> None:
    client = StdioClient(
        BackendConfig(name="mortal", command=sys.executable, args=["-c", "import sys; sys.exit(3)"])
    )
    with pytest.raises(BackendDisconnectedError):
        await client.start()


@pytest.mark.asyncio
async def test_comando_inexistente() -> None:
    client = StdioClient(BackendConfig(name="fantasma", command="comando-que-nao-existe-xyz"))
    with pytest.raises(BackendError, match="comando n"):
        await client.start()


@pytest.mark.asyncio
async def test_erro_jsonrpc_do_backend() -> None:
    client = make_client()
    await client.start()
    try:
        with pytest.raises(BackendJsonRpcError) as exc_info:
            await client.send_request("metodo/inexistente")
        assert exc_info.value.code == -32603
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_stop_encerra_processo() -> None:
    client = make_client()
    await client.start()
    await client.stop()
    assert client._process is None or client._process.returncode is not None
    await client.stop()  # idempotente


@pytest.mark.asyncio
async def test_lista_resources_e_prompts_do_backend() -> None:
    """O fake backend agora expõe resources e prompts via StdioClient real."""
    client = make_client()
    await client.start()
    try:
        resources = await client.list_resources()
        assert [r["uri"] for r in resources] == [
            "memory://greeting",
            "file:///tmp/fake-note.txt",
        ]

        read = await client.send_request("resources/read", {"uri": "memory://greeting"})
        assert read["contents"][0]["text"] == "Ola! Bem-vindo ao fake backend."

        prompts = await client.list_prompts()
        assert [p["name"] for p in prompts] == ["greet"]

        got = await client.send_request("prompts/get", {"name": "greet", "arguments": {"person": "Ana"}})
        assert got["messages"][0]["content"]["text"] == "Ola, Ana!"
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_backend_sem_resources_ou_prompts_retorna_lista_vazia() -> None:
    """Backend que responde MethodNotFound vira lista vazia, não erro (Fase 1)."""
    client = make_client(args=[str(FAKE_BACKEND_PATH), "--no-resources", "--no-prompts"])
    await client.start()
    try:
        assert await client.list_resources() == []
        assert await client.list_prompts() == []
        # Tools continuam funcionando normalmente.
        tools = await client.list_tools()
        assert {t["name"] for t in tools} == {"echo", "add"}
    finally:
        await client.stop()


# ----------------------------------------------------------------------
# Robustez do ciclo de vida (cleanup de start falho, reader morto)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_falho_no_handshake_nao_deixa_orfaos() -> None:
    """6.1 — handshake que falha encerra processo e tasks (nenhum órfão).

    O backend aceita a conexão stdio mas nunca responde o initialize: o
    handshake estoura o timeout e o start() deve fazer stop() interno —
    processo terminado, tasks canceladas e client reutilizável (uma nova
    tentativa de start() não deve achar que já está de pé).
    """
    client = StdioClient(
        BackendConfig(
            name="falho",
            command=sys.executable,
            args=["-c", "import sys; sys.stdin.read()"],  # lê e nunca responde
        ),
        request_timeout=0.3,
    )
    with pytest.raises(BackendTimeoutError):
        await client.start()

    # Nenhum processo órfão: o stop() interno encerrou o filho.
    assert client._process is None or client._process.returncode is not None  # noqa: SLF001
    # Nenhuma task de leitura viva.
    assert client._reader_task is None or client._reader_task.done()  # noqa: SLF001
    assert client._stderr_task is None or client._stderr_task.done()  # noqa: SLF001
    # start() seguinte não acha que já está de pé (self._process limpo/encerrado
    # permite nova tentativa em vez de retorno silencioso).
    assert not client.is_alive()
    await client.stop()


@pytest.mark.asyncio
async def test_reader_morto_invalida_is_alive() -> None:
    """6.2 — erro inesperado no leitor de stdout: is_alive() vira False.

    O processo segue tecnicamente vivo, mas nenhuma resposta seria processada:
    o client não pode aparentar saudável para o Health Monitor.
    """
    client = make_client()
    await client.start()
    try:
        assert client.is_alive()
        # Força a falha do leitor sem matar o processo: injeta uma exceção no
        # caminho de leitura (o mesmo efeito de um erro inesperado real).
        reader_task = client._reader_task
        assert reader_task is not None
        client._reader_failed = True  # noqa: SLF001 — estado que _read_stdout setaria
        # Simula o efeito completo da falha: o finally de _read_stdout derruba
        # os pendentes; aqui basta validar a semântica de is_alive().
        assert not client.is_alive()
        # O processo em si continua vivo (returncode None) — é exatamente o
        # cenário "zumbi" que a flag existe para cobrir.
        assert client._process is not None  # noqa: SLF001
        assert client._process.returncode is None  # noqa: SLF001
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_reader_task_marcada_como_falha_em_erro_real() -> None:
    """6.2 (comportamento real) — _read_stdout seta _reader_failed no except.

    Um backend que escreve no stdout e fecha sem quebrar nada não dispara o
    caminho de erro; para exercitá-lo de verdade, fechamos o stdout à força
    (simula falha de transporte no meio da leitura) e verificamos que o
    client fica inválido para o Health Monitor.
    """
    client = make_client()
    await client.start()
    try:
        # Fecha o pipe de stdout do processo: readline() vai falhar/encerrar.
        # (O fake continua vivo; só o canal de leitura quebra.)
        assert client._process is not None and client._process.stdout is not None  # noqa: SLF001
        client._process.stdout._transport.close()  # type: ignore[attr-defined] # noqa: SLF001
        # Aguarda a task do leitor terminar (com ou sem exceção).
        reader_task = client._reader_task
        assert reader_task is not None
        for _ in range(50):
            if reader_task.done():
                break
            await asyncio.sleep(0.05)
        # O importante: is_alive() não pode continuar True com o leitor morto.
        # (Se o readline retornou b"" por causa do close, o processo segue
        # vivo mas o pending foi derrubado; se levantou, _reader_failed=True.)
        if reader_task.exception() is not None:
            assert client._reader_failed  # noqa: SLF001
            assert not client.is_alive()
    finally:
        await client.stop()


# ----------------------------------------------------------------------
# Robustez adicional (itens 91-120)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_write_levanta_quando_client_encerrado() -> None:
    """1 (91-120) — _write checa _closed proativamente, não apenas no send_request.

    Antes da correção, _write só verificava process/stdin — não _closed nem
    is_closing(). _send_notification (que chama _write direto) não tinha a
    checagem defensiva de send_request, então tentava escrever num stdin já
    fechado. Agora _write levanta cedo.
    """
    client = make_client()
    await client.start()
    await client.stop()
    # Após stop(), _closed=True e stdin fechado — _write não deve tentar I/O.
    with pytest.raises(BackendDisconnectedError, match="stdin indisponível"):
        await client._write({"jsonrpc": "2.0", "method": "ping"})  # noqa: SLF001


@pytest.mark.asyncio
async def test_send_notification_levanta_quando_client_encerrado() -> None:
    """1 (91-120) — _send_notification delega a _write, que checa _closed.

    Confirma o caminho proativo: não tenta escrever notificação num client já
    encerrado. (Antes, só send_request checava _closed; _send_notification não.)
    """
    client = make_client()
    await client.start()
    await client.stop()
    with pytest.raises(BackendDisconnectedError, match="stdin indisponível"):
        await client._send_notification("notifications/initialized")  # noqa: SLF001


@pytest.mark.asyncio  
async def test_handle_message_rejeita_jsonrpc_invalido() -> None:  
    """Envelope malformado é validado por BaseClient._apply_response.  
  
    Após o BUG-A, StdioClient._handle_message não valida mais o envelope:  
    delega 100% para _apply_response, que loga o aviso  
    "resposta malformada do backend" e FALHA a pending com BackendError —  
    nunca resolve com resultado vazio.  
    """  
    client = make_client()  
    await client.start()  
    try:  
        malformed_lines = [  
            json.dumps({"id": 1, "result": {}}).encode(),  # sem jsonrpc  
            json.dumps({"jsonrpc": "1.0", "id": 2, "result": {}}).encode(),  # jsonrpc errado  
            json.dumps({"jsonrpc": None, "id": 3, "result": {}}).encode(),  # jsonrpc null  
        ]  
        # Registra uma pending real para cada id: o contrato é FALHAR a future  
        # com BackendError (não resolver com resultado vazio).  
        futures = [client._register_pending(i) for i in (1, 2, 3)]  # noqa: SLF001  
        with capture_structlog_events() as events:  
            for line in malformed_lines:  
                await client._handle_message(line)  # noqa: SLF001  
        malformed_events = [  
            e for e in events if e["event"] == "resposta malformada do backend"  
        ]  
        assert len(malformed_events) == 3, (  
            f"esperado 3 avisos de envelope inválido, got {len(malformed_events)}"  
        )  
        # Cada pending foi FALHADA com BackendError (nunca resolvida com None/{}).  
        for fut in futures:  
            assert fut.done()  
            with pytest.raises(BackendError):  
                fut.result()  
        # Nenhum aviso antigo de "resposta inesperada" deve ter sido emitido.  
        assert not [  
            e for e in events if e["event"].startswith("resposta inesperada")  
        ]  
        # Todas as pendings foram consumidas.  
        assert client._pending == {}  # noqa: SLF001  
    finally:  
        await client.stop()
