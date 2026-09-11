"""Testes unitários do McpServer (clients fake, sem I/O)."""  
  
import pytest  
from gateway import __version__  
  
from conftest import FakeClient, make_manager_for_clients  
from gateway.models import (  
    BACKEND_UNAVAILABLE,  
    INVALID_PARAMS,  
    INVALID_REQUEST,  
    ITEM_NOT_FOUND,  
    METHOD_NOT_FOUND,  
)  
from gateway.server import McpServer  
  
ECHO_TOOL = {  
    "name": "echo",  
    "description": "Repete texto.",  
    "inputSchema": {"type": "object", "properties": {}},  
}  
ADD_TOOL = {  
    "name": "add",  
    "description": "Soma.",  
    "inputSchema": {"type": "object", "properties": {}},  
}  
RESOURCE_ITEM = {  
    "uri": "file:///tmp/a.txt",  
    "name": "a.txt",  
    "description": "Arquivo A.",  
    "mimeType": "text/plain",  
}  
PROMPT_ITEM = {  
    "name": "greet",  
    "description": "Sauda alguém.",  
    "arguments": [{"name": "person", "description": "Quem saudar.", "required": True}],  
}  
  
  
async def make_server(  
    client_a: FakeClient | None = None, client_b: FakeClient | None = None  
) -> tuple[McpServer, FakeClient, FakeClient]:  
    """Sobe um McpServer com dois clients fake e devolve (server, a, b)."""  
    if client_a is None:  
        client_a = FakeClient(tools=[ECHO_TOOL])  
    if client_b is None:  
        client_b = FakeClient(tools=[ADD_TOOL])  
    manager, registries = make_manager_for_clients(  
        {"backend-a": client_a, "backend-b": client_b}  
    )  
    server = McpServer(manager, registries)  
    await server.start()  
    return server, client_a, client_b  
  
  
async def make_rich_server() -> tuple[McpServer, FakeClient, FakeClient]:  
    """McpServer em que backend-a tem resources/prompts e backend-b não."""  
    client_a = FakeClient(tools=[ECHO_TOOL], resources=[RESOURCE_ITEM], prompts=[PROMPT_ITEM])  
    client_b = FakeClient(tools=[ADD_TOOL])  
    manager, registries = make_manager_for_clients(  
        {"backend-a": client_a, "backend-b": client_b}  
    )  
    server = McpServer(manager, registries)  
    await server.start()  
    return server, client_a, client_b  
  
  
@pytest.mark.asyncio  
async def test_tools_list_agregado() -> None:  
    """tools/list agrega com namespace e preserva o schema original (descrição/inputSchema)."""  
    server, _, _ = await make_server()  
    response = await server.process_message({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})  
    assert response is not None  
    assert response["id"] == 1  
    assert response.get("error") is None  
    tool_names = {tool["name"] for tool in response["result"]["tools"]}  
    assert tool_names == {"backend-a.echo", "backend-b.add"}  
    by_name = {tool["name"]: tool for tool in response["result"]["tools"]}  
    assert by_name["backend-a.echo"]["description"] == "Repete texto."  
  
  
@pytest.mark.asyncio  
async def test_tools_call_roteia_para_backend_certo() -> None:  
    """tools/call roteia pelo namespace: o backend certo recebe o nome ORIGINAL da tool.  
  
    Confirma também que no startup o McpServer pede as três listagens  
    (tools/resources/prompts) de cada backend.  
    """  
    server, client_a, client_b = await make_server()  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 2,  
            "method": "tools/call",  
            "params": {"name": "backend-a.echo", "arguments": {"text": "oi"}},  
        }  
    )  
    assert response is not None  
    assert response["result"]["content"][0]["text"] == "resultado de echo"  
    assert ("tools/call", {"name": "echo", "arguments": {"text": "oi"}}) in client_a.requests  
    assert client_b.requests == [  
        ("tools/list", None),  
        ("resources/list", None),  
        ("prompts/list", None),  
    ]  
  
  
@pytest.mark.asyncio  
async def test_tools_call_tool_inexistente() -> None:  
    server, _, _ = await make_server()  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 3,  
            "method": "tools/call",  
            "params": {"name": "backend-a.nao-existe", "arguments": {}},  
        }  
    )  
    assert response is not None  
    assert response["error"]["code"] == ITEM_NOT_FOUND  
    assert "Unknown tool" in response["error"]["message"]  
  
  
@pytest.mark.asyncio  
async def test_tools_call_sem_name() -> None:  
    server, _, _ = await make_server()  
    response = await server.process_message(  
        {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {}}  
    )  
    assert response is not None  
    assert response["error"]["code"] == INVALID_PARAMS  
  
  
@pytest.mark.asyncio  
async def test_method_desconhecido() -> None:  
    server, _, _ = await make_server()  
    response = await server.process_message(  
        {"jsonrpc": "2.0", "id": 5, "method": "recursos/desconhecido"}  
    )  
    assert response is not None  
    assert response["error"]["code"] == METHOD_NOT_FOUND  
    assert response["id"] == 5  
  
  
@pytest.mark.asyncio  
async def test_request_com_campo_obrigatorio_ausente() -> None:  
    server, _, _ = await make_server()  
    response = await server.process_message({"jsonrpc": "2.0", "id": 6})  # sem method  
    assert response is not None  
    assert response["error"]["code"] == INVALID_REQUEST  
    assert "method" in response["error"]["message"]  
  
    response = await server.process_message({"id": 7, "method": "tools/list"})  # sem jsonrpc  
    assert response is not None  
    assert response["error"]["code"] == INVALID_REQUEST  
    assert "jsonrpc" in response["error"]["message"]  
  
  
@pytest.mark.asyncio  
async def test_request_com_id_invalido() -> None:  
    server, _, _ = await make_server()  
    response = await server.process_message({"jsonrpc": "2.0", "id": {"nope": 1}, "method": "ping"})  
    assert response is not None  
    assert response["error"]["code"] == INVALID_REQUEST  
    assert response["id"] is None  
  
  
@pytest.mark.asyncio  
async def test_notificacao_nao_responde() -> None:  
    server, _, _ = await make_server()  
    response = await server.process_message(  
        {"jsonrpc": "2.0", "method": "notifications/initialized"}  
    )  
    assert response is None  
  
  
@pytest.mark.asyncio  
async def test_initialize() -> None:  
    """initialize devolve serverInfo/version e anuncia tools, resources e prompts (Fase 1)."""  
    server, _, _ = await make_server()  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 7,  
            "method": "initialize",  
            "params": {  
                "protocolVersion": "2024-11-05",  
                "capabilities": {},  
                "clientInfo": {"name": "test-client", "version": "1.0"},  
            },  
        }  
    )  
    assert response is not None  
    assert response["result"]["serverInfo"]["name"] == "mcp-gateway"  
    assert response["result"]["serverInfo"]["version"] == __version__  
    assert response["result"]["capabilities"]["tools"]["listChanged"] is False  
    assert "resources" in response["result"]["capabilities"]  
    assert "prompts" in response["result"]["capabilities"]  
  
  
@pytest.mark.asyncio  
@pytest.mark.parametrize(  
    "params",  
    [  
        {},  
        {"protocolVersion": "2024-11-05", "capabilities": {}},  
        {  
            "protocolVersion": "2024-11-05",  
            "capabilities": [],  
            "clientInfo": {"name": "client", "version": "1"},  
        },  
        {  
            "protocolVersion": "unsupported",  
            "capabilities": {},  
            "clientInfo": {"name": "client", "version": "1"},  
        },  
    ],  
)  
async def test_initialize_rejeita_payload_ou_versao_invalida(  
    params: dict[str, object],  
) -> None:  
    server, _, _ = await make_server()  
    response = await server.process_message(  
        {"jsonrpc": "2.0", "id": 70, "method": "initialize", "params": params}  
    )  
  
    assert response is not None  
    assert response["error"]["code"] == INVALID_PARAMS  
  
  
@pytest.mark.asyncio  
async def test_notification_id_null_e_id_normal_recebem_resposta() -> None:  
    server, _, _ = await make_server()  
  
    notification = await server.process_message(  
        {"jsonrpc": "2.0", "method": "ping"}  
    )  
    null_id = await server.process_message(  
        {"jsonrpc": "2.0", "id": None, "method": "ping"}  
    )  
    regular_id = await server.process_message(  
        {"jsonrpc": "2.0", "id": 71, "method": "ping"}  
    )  
  
    assert notification is None  
    assert null_id == {"jsonrpc": "2.0", "id": None, "result": {}}  
    assert regular_id is not None  
    assert regular_id["id"] == 71  
  
  
@pytest.mark.asyncio  
@pytest.mark.parametrize("method", ["tools/call", "resources/read", "prompts/get"])  
async def test_handlers_rejeitam_params_que_nao_sao_objeto(method: str) -> None:  
    server, _, _ = await make_server()  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 72,  
            "method": method,  
            "params": [1, 2, 3],  
        }  
    )  
  
    assert response is not None  
    assert response["error"]["code"] == INVALID_PARAMS  
  
  
@pytest.mark.asyncio  
async def test_set_active_backends_rejeita_params_que_nao_sao_objeto() -> None:  
    server, _, _ = await make_server()  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 73,  
            "method": "gateway/session/set_active_backends",  
            "params": [1],  
        },  
        session_id="session",  
    )  
  
    assert response is not None  
    assert response["error"]["code"] == INVALID_PARAMS  
  
  
@pytest.mark.asyncio  
async def test_listagem_omite_metadata_invalida_sem_derrupar_gateway() -> None:  
    server, _, _ = await make_server(  
        client_a=FakeClient(  
            tools=[{"name": "missing-description"}],  
            resources=[{"uri": "memory://missing-name"}],  
            prompts=[{"name": "valid-prompt"}],  
        )  
    )  
  
    tools = await server.process_message(  
        {"jsonrpc": "2.0", "id": 74, "method": "tools/list"}  
    )  
    resources = await server.process_message(  
        {"jsonrpc": "2.0", "id": 75, "method": "resources/list"}  
    )  
    prompts = await server.process_message(  
        {"jsonrpc": "2.0", "id": 76, "method": "prompts/list"}  
    )  
  
    assert tools is not None and tools["result"]["tools"] == [{"name": "backend-b.add", "description": "Soma.", "inputSchema": {"type": "object", "properties": {}}}]  
    assert resources is not None and resources["result"]["resources"] == []  
    assert prompts is not None and [item["name"] for item in prompts["result"]["prompts"]] == ["backend-a.valid-prompt"]  
  
  
@pytest.mark.asyncio  
async def test_falha_do_backend_vira_erro_de_aplicacao() -> None:  
    failing = FakeClient(tools=[ECHO_TOOL], fail_calls=True)  
    server, _, _ = await make_server(client_a=failing)  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 8,  
            "method": "tools/call",  
            "params": {"name": "backend-a.echo", "arguments": {}},  
        }  
    )  
    assert response is not None  
    assert response["error"]["code"] == BACKEND_UNAVAILABLE  
    assert "backend-a" in response["error"]["message"]  
  
  
@pytest.mark.asyncio  
async def test_resources_list_agregado_com_namespace_de_uri() -> None:  
    """resources/list namespaceia a URI (backend.<uri>); backend sem resources não contribui."""  
    server, _, _ = await make_rich_server()  
    response = await server.process_message({"jsonrpc": "2.0", "id": 10, "method": "resources/list"})  
    assert response is not None  
    assert response.get("error") is None  
    by_uri = {res["uri"]: res for res in response["result"]["resources"]}  
    assert set(by_uri) == {"backend-a.file:///tmp/a.txt"}  
    entry = by_uri["backend-a.file:///tmp/a.txt"]  
    assert entry["name"] == "a.txt"  
    assert entry["mimeType"] == "text/plain"  
  
  
@pytest.mark.asyncio  
async def test_resources_list_vazio_quando_ninguem_tem_resources() -> None:  
    server, _, _ = await make_server()  # nenhum backend com resources  
    response = await server.process_message({"jsonrpc": "2.0", "id": 11, "method": "resources/list"})  
    assert response is not None  
    assert response["result"]["resources"] == []  
  
  
@pytest.mark.asyncio  
async def test_resources_read_roteia_uri_original_e_namespaceia_contents() -> None:  
    """resources/read roteia a URI ORIGINAL ao backend e re-namespaceia a URI no contents (round-trip)."""  
    server, client_a, _ = await make_rich_server()  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 12,  
            "method": "resources/read",  
            "params": {"uri": "backend-a.file:///tmp/a.txt"},  
        }  
    )  
    assert response is not None  
    assert response.get("error") is None  
    assert ("resources/read", {"uri": "file:///tmp/a.txt"}) in client_a.requests  
    assert response["result"]["contents"][0]["uri"] == "backend-a.file:///tmp/a.txt"  
    assert response["result"]["contents"][0]["text"] == "conteúdo de file:///tmp/a.txt"  
  
  
@pytest.mark.asyncio  
async def test_resources_read_uri_inexistente() -> None:  
    server, _, _ = await make_rich_server()  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 13,  
            "method": "resources/read",  
            "params": {"uri": "backend-b.file:///tmp/nope.txt"},  
        }  
    )  
    assert response is not None  
    assert response["error"]["code"] == ITEM_NOT_FOUND  
    assert "Unknown resource" in response["error"]["message"]  
  
  
@pytest.mark.asyncio  
async def test_resources_read_sem_uri() -> None:  
    server, _, _ = await make_rich_server()  
    response = await server.process_message(  
        {"jsonrpc": "2.0", "id": 14, "method": "resources/read", "params": {}}  
    )  
    assert response is not None  
    assert response["error"]["code"] == INVALID_PARAMS  
  
  
@pytest.mark.asyncio  
async def test_resources_read_com_backend_fora_do_ar() -> None:  
    failing = FakeClient(tools=[ECHO_TOOL], resources=[RESOURCE_ITEM], fail_method="resources/read")  
    server, _, _ = await make_server(client_a=failing)  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 15,  
            "method": "resources/read",  
            "params": {"uri": "backend-a.file:///tmp/a.txt"},  
        }  
    )  
    assert response is not None  
    assert response["error"]["code"] == BACKEND_UNAVAILABLE  
  
  
@pytest.mark.asyncio  
async def test_prompts_list_agregado_com_namespace() -> None:  
    server, _, _ = await make_rich_server()  
    response = await server.process_message({"jsonrpc": "2.0", "id": 20, "method": "prompts/list"})  
    assert response is not None  
    assert response.get("error") is None  
    by_name = {p["name"]: p for p in response["result"]["prompts"]}  
    assert set(by_name) == {"backend-a.greet"}  
    assert by_name["backend-a.greet"]["arguments"][0]["name"] == "person"  
  
  
@pytest.mark.asyncio  
async def test_prompts_get_roteia_nome_original_e_argumentos() -> None:  
    server, client_a, _ = await make_rich_server()  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 21,  
            "method": "prompts/get",  
            "params": {"name": "backend-a.greet", "arguments": {"person": "Ana"}},  
        }  
    )  
    assert response is not None  
    assert response.get("error") is None  
    assert ("prompts/get", {"name": "greet", "arguments": {"person": "Ana"}}) in client_a.requests  
    assert response["result"]["messages"][0]["content"]["text"] == "olá greet"  
  
  
@pytest.mark.asyncio  
async def test_prompts_get_prompt_inexistente() -> None:  
    server, _, _ = await make_rich_server()  
    response = await server.process_message(  
        {  
            "jsonrpc": "2.0",  
            "id": 22,  
            "method": "prompts/get",  
            "params": {"name": "backend-b.nao-existe"},  
        }  
    )  
    assert response is not None  
    assert response["error"]["code"] == ITEM_NOT_FOUND  
  
  
@pytest.mark.asyncio  
async def test_prompts_get_sem_name() -> None:  
    server, _, _ = await make_rich_server()  
    response = await server.process_message(  
        {"jsonrpc": "2.0", "id": 23, "method": "prompts/get", "params": {}}  
    )  
    assert response is not None  
    assert response["error"]["code"] == INVALID_PARAMS