"""Testes do filtro seletivo de backends por sessão (Fase 5).

Eixo central: **não-regressão** — sem ``Mcp-Session-Id``/sem filtro, o
comportamento é bit a bit o das fases anteriores (teste explícito abaixo).
O resto cobre o protocolo da extensão, isolamento entre sessões, o erro
"unknown tool" idêntico para tools bloqueadas e a expiração com clock fake.
"""

import httpx
import pytest

from conftest import FakeClient, make_manager_for_clients
from gateway.http_server import create_app
from gateway.server import McpServer
from gateway.sessions import SessionFilter

ECHO_TOOL = {
    "name": "echo",
    "description": "Repete texto.",
    "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}},
}
ADD_TOOL = {
    "name": "add",
    "description": "Soma.",
    "inputSchema": {"type": "object", "properties": {}},
}
GREET_RESOURCE = {"uri": "memory://greet", "name": "greet", "mimeType": "text/plain"}
GREET_PROMPT = {"name": "greet", "description": "Saudação."}

BASE_URL = "http://test"


class FakeClock:
    """Relógio monotônico controlável (mock de tempo para o TTL)."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_server(
    session_filter: SessionFilter | None = None,
) -> tuple[McpServer, dict[str, FakeClient]]:
    """McpServer com 2 backends fake (tools, resources e prompts)."""
    clients = {
        "backend-a": FakeClient(
            tools=[ECHO_TOOL], resources=[GREET_RESOURCE], prompts=[GREET_PROMPT]
        ),
        "backend-b": FakeClient(
            tools=[ADD_TOOL], resources=[dict(GREET_RESOURCE, uri="memory://b")], prompts=[]
        ),
    }
    manager, registries = make_manager_for_clients(clients)
    server = McpServer(manager, registries, session_filter=session_filter)
    return server, clients


async def rpc(
    server: McpServer,
    method: str,
    params: dict | None = None,
    request_id: int = 1,
    session_id: str | None = None,
) -> dict:
    body = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        body["params"] = params
    return await server.process_message(body, session_id=session_id) or {}


# ----------------------------------------------------------------------
# NÃO-REGRESSÃO: sem sessão, tudo como antes
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sem_sessao_comportamento_identico_ao_anterior() -> None:
    """Sem header/sessão: tools/list, call, resources e prompts como na Fase 4."""
    server, _ = make_server()
    await server.start()
    try:
        response = await rpc(server, "tools/list")
        names = {t["name"] for t in response["result"]["tools"]}
        assert names == {"backend-a.echo", "backend-b.add"}  # TUDO, como antes

        response = await rpc(server, "resources/list")
        uris = {r["uri"] for r in response["result"]["resources"]}
        assert uris == {"backend-a.memory://greet", "backend-b.memory://b"}

        response = await rpc(server, "prompts/list")
        assert {p["name"] for p in response["result"]["prompts"]} == {"backend-a.greet"}

        # Chamada a qualquer backend funciona sem filtro.
        response = await rpc(
            server,
            "tools/call",
            {"name": "backend-a.echo", "arguments": {"text": "oi"}},
        )
        assert response.get("error") is None
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_sessao_sem_filtro_vira_acesso_total() -> None:
    """Sessão COM header mas SEM set_active_backends: idêntico a sem sessão."""
    server, _ = make_server()
    await server.start()
    try:
        response = await rpc(server, "tools/list", session_id="sess-sem-filtro")
        names = {t["name"] for t in response["result"]["tools"]}
        assert names == {"backend-a.echo", "backend-b.add"}
        # get_active_backends confirma: sem filtro.
        response = await rpc(
            server, "gateway/session/get_active_backends", session_id="sess-sem-filtro"
        )
        assert response["result"] == {"active_backends": None, "filtered": False}
    finally:
        await server.stop()


# ----------------------------------------------------------------------
# Protocolo da extensão
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_active_backends_sem_sessao_retorna_invalid_request() -> None:
    server, _ = make_server()
    await server.start()
    try:
        response = await rpc(
            server, "gateway/session/set_active_backends", {"backends": ["backend-a"]}
        )
        assert response["error"]["code"] == -32600
        assert "Mcp-Session-Id" in response["error"]["message"]
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_set_active_backends_validacao_de_params() -> None:
    server, _ = make_server()
    await server.start()
    try:
        for bad in (
            None,
            {},
            {"backends": []},
            {"backends": "backend-a"},  # string, não lista
            {"backends": ["backend-a", ""]},
            {"backends": ["backend-a", 42]},
        ):
            response = await rpc(
                server,
                "gateway/session/set_active_backends",
                bad,
                session_id="s",
            )
            assert response["error"]["code"] == -32602, bad

        # Backend desconhecido: falha EXPLÍCITA (nunca filtro parcial silencioso).
        response = await rpc(
            server,
            "gateway/session/set_active_backends",
            {"backends": ["backend-a", "fantasma"]},
            session_id="s",
        )
        assert response["error"]["code"] == -32602
        assert "fantasma" in response["error"]["message"]
        assert response["error"]["data"]["known_backends"] == ["backend-a", "backend-b"]
        # E o filtro NÃO foi aplicado.
        response = await rpc(server, "gateway/session/get_active_backends", session_id="s")
        assert response["result"]["filtered"] is False
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_set_retorna_lista_e_get_confirma() -> None:
    server, _ = make_server()
    await server.start()
    try:
        response = await rpc(
            server,
            "gateway/session/set_active_backends",
            {"backends": ["backend-b", "backend-a"]},
            session_id="s",
        )
        assert response["result"]["active_backends"] == ["backend-a", "backend-b"]
        response = await rpc(server, "gateway/session/get_active_backends", session_id="s")
        assert response["result"] == {
            "active_backends": ["backend-a", "backend-b"],
            "filtered": True,
        }
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_clear_volta_a_ver_tudo() -> None:
    server, _ = make_server()
    await server.start()
    try:
        await rpc(
            server, "gateway/session/set_active_backends", {"backends": ["backend-a"]},
            session_id="s",
        )
        response = await rpc(server, "tools/list", session_id="s")
        assert {t["name"] for t in response["result"]["tools"]} == {"backend-a.echo"}

        await rpc(server, "gateway/session/clear_active_backends", session_id="s")
        response = await rpc(server, "tools/list", session_id="s")
        assert {t["name"] for t in response["result"]["tools"]} == {
            "backend-a.echo",
            "backend-b.add",
        }
    finally:
        await server.stop()


# ----------------------------------------------------------------------
# Filtragem de listagens e chamadas
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filtro_aplica_a_tools_resources_e_prompts() -> None:
    server, _ = make_server()
    await server.start()
    try:
        await rpc(
            server, "gateway/session/set_active_backends", {"backends": ["backend-a"]},
            session_id="s",
        )
        tools = await rpc(server, "tools/list", session_id="s")
        assert {t["name"] for t in tools["result"]["tools"]} == {"backend-a.echo"}
        resources = await rpc(server, "resources/list", session_id="s")
        assert {r["uri"] for r in resources["result"]["resources"]} == {
            "backend-a.memory://greet"
        }
        prompts = await rpc(server, "prompts/list", session_id="s")
        assert {p["name"] for p in prompts["result"]["prompts"]} == {"backend-a.greet"}
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tools_call_fora_do_filtro_erro_idem_inexistente() -> None:
    """Tool bloqueada pelo filtro responde EXATAMENTE como inexistente."""
    server, _ = make_server()
    await server.start()
    try:
        await rpc(
            server, "gateway/session/set_active_backends", {"backends": ["backend-a"]},
            session_id="s",
        )
        blocked = await rpc(
            server,
            "tools/call",
            {"name": "backend-b.add", "arguments": {"a": 1, "b": 2}},
            session_id="s",
            request_id=10,
        )
        unknown = await rpc(
            server,
            "tools/call",
            {"name": "backend-b.nunca-existiu", "arguments": {}},
            session_id="s",
            request_id=11,
        )
        # Mesmo código e mesma FORMA de mensagem: o filtro não vaza a
        # existência — para a sessão, tool bloqueada é indistinguível de
        # inexistente (o nome da tool obviamente difere na mensagem).
        assert blocked["error"]["code"] == unknown["error"]["code"] == -32001
        assert blocked["error"]["message"].startswith("Unknown tool:")
        assert unknown["error"]["message"].startswith("Unknown tool:")

        # resources/read e prompts/get idem.
        res_blocked = await rpc(
            server, "resources/read", {"uri": "backend-b.memory://b"}, session_id="s"
        )
        assert res_blocked["error"]["code"] == -32001
        assert res_blocked["error"]["message"].startswith("Unknown resource:")
        prompt_blocked = await rpc(
            server, "prompts/get", {"name": "backend-b.qualquer"}, session_id="s"
        )
        assert prompt_blocked["error"]["code"] == -32001

        # A tool do backend PERMITIDO continua funcionando.
        ok = await rpc(
            server,
            "tools/call",
            {"name": "backend-a.echo", "arguments": {"text": "oi"}},
            session_id="s",
            request_id=12,
        )
        assert ok.get("error") is None
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_sessoes_isoladas_com_filtros_diferentes() -> None:
    """Duas sessões com filtros diferentes + uma sem filtro: nenhuma interfere."""
    server, _ = make_server()
    await server.start()
    try:
        await rpc(
            server, "gateway/session/set_active_backends", {"backends": ["backend-a"]},
            session_id="sess-a",
        )
        await rpc(
            server, "gateway/session/set_active_backends", {"backends": ["backend-b"]},
            session_id="sess-b",
        )

        names_a = {
            t["name"] for t in (await rpc(server, "tools/list", session_id="sess-a"))["result"]["tools"]
        }
        names_b = {
            t["name"] for t in (await rpc(server, "tools/list", session_id="sess-b"))["result"]["tools"]
        }
        names_all = {
            t["name"] for t in (await rpc(server, "tools/list", session_id="sess-tudo"))["result"]["tools"]
        }
        names_none = {
            t["name"] for t in (await rpc(server, "tools/list"))["result"]["tools"]
        }
        assert names_a == {"backend-a.echo"}
        assert names_b == {"backend-b.add"}
        assert names_all == {"backend-a.echo", "backend-b.add"}  # sessão sem filtro
        assert names_none == {"backend-a.echo", "backend-b.add"}  # sem header
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_filtro_reflete_backend_removido_do_registry() -> None:
    """Backend que cai (sai do registry) some da view filtrada sem erro."""
    server, clients = make_server()
    await server.start()
    try:
        await rpc(
            server, "gateway/session/set_active_backends", {"backends": ["backend-b"]},
            session_id="s",
        )
        await clients["backend-b"].stop()  # backend cai; registros saem do registry
        from gateway.backend_manager import BackendStatus

        state = server.backend_manager.get_state("backend-b")
        state.status = BackendStatus.OFFLINE
        state.client = None
        server.backend_manager._unregister("backend-b")

        response = await rpc(server, "tools/list", session_id="s")
        assert response["result"]["tools"] == []  # view filtrada fica vazia, sem erro
    finally:
        await server.stop()


# ----------------------------------------------------------------------
# TTL / expiração (clock fake, sem espera real)
# ----------------------------------------------------------------------


def test_sessao_expira_apos_ttl_e_volta_a_ver_tudo() -> None:
    clock = FakeClock()
    sessions = SessionFilter(ttl_seconds=60.0, clock=clock)
    sessions.set_active_backends("s", frozenset({"backend-a"}))
    assert sessions.active_backends("s") == frozenset({"backend-a"})

    clock.advance(59.9)
    assert sessions.active_backends("s") == frozenset({"backend-a"})  # dentro do TTL
    # Este acesso RENOVOU o deadline (agora é 59.9 + 60 a partir de então).

    clock.advance(60.0)  # 60s após o último acesso, sem atividade nova
    assert sessions.active_backends("s") is None  # expirada: volta a ver tudo


def test_atividade_renova_o_ttl() -> None:
    clock = FakeClock()
    sessions = SessionFilter(ttl_seconds=60.0, clock=clock)
    sessions.set_active_backends("s", frozenset({"backend-a"}))
    for _ in range(10):  # acessos a cada 50s: nunca expira
        clock.advance(50.0)
        assert sessions.active_backends("s") == frozenset({"backend-a"})


def test_purge_remove_apenas_expiradas() -> None:
    clock = FakeClock()
    sessions = SessionFilter(ttl_seconds=60.0, clock=clock)
    sessions.set_active_backends("velha", frozenset({"backend-a"}))
    clock.advance(100.0)
    sessions.set_active_backends("nova", frozenset({"backend-b"}))
    removed = sessions.purge_expired()
    assert removed == 1
    assert sessions.session_count() == 1
    assert sessions.active_backends("nova") == frozenset({"backend-b"})
    assert sessions.active_backends("velha") is None


@pytest.mark.asyncio
async def test_sessao_expirada_no_gateway_vira_acesso_total() -> None:
    """Gateway com TTL curto: sessão expirada vê tudo (comportamento seguro)."""
    clock = FakeClock()
    server, _ = make_server(session_filter=SessionFilter(1.0, clock=clock))
    await server.start()
    try:
        await rpc(
            server, "gateway/session/set_active_backends", {"backends": ["backend-a"]},
            session_id="s",
        )
        response = await rpc(server, "tools/list", session_id="s")
        assert {t["name"] for t in response["result"]["tools"]} == {"backend-a.echo"}

        clock.advance(2.0)  # TTL de 1s estourado
        response = await rpc(server, "tools/list", session_id="s")
        assert {t["name"] for t in response["result"]["tools"]} == {
            "backend-a.echo",
            "backend-b.add",
        }
    finally:
        await server.stop()


# ----------------------------------------------------------------------
# HTTP: header Mcp-Session-Id e endpoint de diagnóstico
# ----------------------------------------------------------------------


async def http_rpc(
    app: object,
    method: str,
    params: dict | None = None,
    session_id: str | None = None,
    token: str | None = None,
) -> httpx.Response:
    body: dict = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params is not None:
        body["params"] = params
    headers = {}
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    if token:
        headers["Authorization"] = f"Bearer {token}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=BASE_URL
    ) as client:
        return await client.post("/mcp", json=body, headers=headers)


@pytest.mark.asyncio
async def test_http_header_de_sessao_controla_o_filtro() -> None:
    """Caminho HTTP real: Mcp-Session-Id no request define a sessão."""
    server, _ = make_server()
    await server.start()
    try:
        app = create_app(server)
        await http_rpc(
            app,
            "gateway/session/set_active_backends",
            {"backends": ["backend-a"]},
            session_id="sess-http",
        )
        resp = await http_rpc(app, "tools/list", session_id="sess-http")
        names = {t["name"] for t in resp.json()["result"]["tools"]}
        assert names == {"backend-a.echo"}

        resp = await http_rpc(app, "tools/list", session_id="outra-sessao")
        names = {t["name"] for t in resp.json()["result"]["tools"]}
        assert names == {"backend-a.echo", "backend-b.add"}

        resp = await http_rpc(app, "tools/list")  # sem header
        assert len(resp.json()["result"]["tools"]) == 2
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_api_tools_size_diagnostico() -> None:
    """/api/tools/size: contagem, chars, tokens aprox., por backend, por sessão."""
    server, _ = make_server()
    await server.start()
    try:
        app = create_app(server, auth_token="segredo")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=BASE_URL
        ) as client:
            resp = await client.get("/api/tools/size")
            assert resp.status_code == 401  # mesma auth do /api/servers

            resp = await client.get("/api/tools/size", headers={"Authorization": "Bearer segredo"})
            assert resp.status_code == 200
            payload = resp.json()
            assert payload["tools_count"] == 2
            assert payload["filtered"] is False
            assert payload["session_id"] is None
            assert payload["json_chars"] > 0
            assert payload["approx_tokens"] == payload["json_chars"] // 4
            assert set(payload["per_backend_chars"]) == {"backend-a", "backend-b"}
            assert sum(payload["per_backend_chars"].values()) < payload["json_chars"]
            # (soma por backend < total: o total inclui o envelope {"tools": [...]})

            # Sessão filtrada: menos tools, menos chars — o ganho mensurável.
            set_resp = await http_rpc(
                app,
                "gateway/session/set_active_backends",
                {"backends": ["backend-a"]},
                session_id="sess-medida",
                token="segredo",
            )
            assert set_resp.status_code == 200, set_resp.text
            resp = await client.get(
                "/api/tools/size",
                headers={
                    "Authorization": "Bearer segredo",
                    "Mcp-Session-Id": "sess-medida",
                },
            )
            payload_filtered = resp.json()
            assert payload_filtered["tools_count"] == 1
            assert payload_filtered["filtered"] is True
            assert payload_filtered["session_id"] == "sess-medida"
            assert (
                payload_filtered["json_chars"] < payload["json_chars"]
            )  # o ponto da fase
    finally:
        await server.stop()


def test_default_session_ttl_seconds() -> None:
    from gateway.config import DEFAULT_SESSION_TTL_SECONDS, GatewayConfig

    config = GatewayConfig(
        backends=[{"name": "a", "command": "x"}],
    )
    assert config.session_ttl_seconds == DEFAULT_SESSION_TTL_SECONDS == 3600.0
