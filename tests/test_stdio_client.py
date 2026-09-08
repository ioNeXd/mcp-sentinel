"""Testes do StdioClient contra o fake_backend.py real (subprocesso)."""

import sys

import pytest

from conftest import FAKE_BACKEND_PATH
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