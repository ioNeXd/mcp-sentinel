#!/usr/bin/env python3  
"""Backend MCP fake (stdio) usado nos testes e na demo local.  
  
Processo Python standalone que fala JSON-RPC 2.0 newline-delimited via  
stdin/stdout: responde ``initialize``, ``tools/list``, ``tools/call``,  
``resources/list``, ``resources/read``, ``prompts/list`` e ``prompts/get``  
com dados fixos. Não depende de nenhuma biblioteca externa.  
  
Flags opcionais simulam backends que não implementam parte do protocolo:  
    --no-resources   resources/list e resources/read respondem MethodNotFound  
    --no-prompts     prompts/list e prompts/get respondem MethodNotFound  
"""  
  
import json  
import os  
import sys  
  
print(f"fake-backend pid={os.getpid()}", file=sys.stderr, flush=True)  
  
PROTOCOL_VERSION = "2024-11-05"  
  
NO_RESOURCES = "--no-resources" in sys.argv  
NO_PROMPTS = "--no-prompts" in sys.argv  
  
TOOLS = [  
    {  
        "name": "echo",  
        "description": "Repete o texto recebido em 'text'.",  
        "inputSchema": {  
            "type": "object",  
            "properties": {  
                "text": {"type": "string", "description": "Texto a ser repetido."}  
            },  
            "required": ["text"],  
        },  
    },  
    {  
        "name": "add",  
        "description": "Soma dois numeros.",  
        "inputSchema": {  
            "type": "object",  
            "properties": {  
                "a": {"type": "number", "description": "Primeira parcela."},  
                "b": {"type": "number", "description": "Segunda parcela."},  
            },  
            "required": ["a", "b"],  
        },  
    },  
]  
  
RESOURCES = [  
    {  
        "uri": "memory://greeting",  
        "name": "greeting",  
        "description": "Saudacao em memoria do fake backend.",  
        "mimeType": "text/plain",  
    },  
    {  
        "uri": "file:///tmp/fake-note.txt",  
        "name": "fake-note.txt",  
        "description": "Nota de exemplo servida pelo fake backend.",  
        "mimeType": "text/plain",  
    },  
]  
  
PROMPTS = [  
    {  
        "name": "greet",  
        "description": "Gera uma saudacao para uma pessoa.",  
        "arguments": [  
            {  
                "name": "person",  
                "description": "Nome de quem sera saudado.",  
                "required": True,  
            }  
        ],  
    }  
]  
  
  
class MethodError(Exception):  
    """Erro JSON-RPC com código próprio (ex.: MethodNotFound de recurso ausente)."""  
  
    def __init__(self, code: int, message: str) -> None:  
        super().__init__(message)  
        self.code = code  
        self.message = message  
  
  
def handle_request(request: dict) -> dict:  
    """Devolve o campo 'result' JSON-RPC para um request conhecido."""  
    method = request.get("method")  
    params = request.get("params") or {}  
    if method == "initialize":  
        capabilities = {"tools": {}}  
        if not NO_RESOURCES:  
            capabilities["resources"] = {}  
        if not NO_PROMPTS:  
            capabilities["prompts"] = {}  
        return {  
            "protocolVersion": PROTOCOL_VERSION,  
            "capabilities": capabilities,  
            "serverInfo": {"name": "fake-backend", "version": "1.0.0"},  
        }  
    if method == "ping":  
        return {}  
    if method == "tools/list":  
        return {"tools": TOOLS}  
    if method == "tools/call":  
        name = params.get("name")  
        arguments = params.get("arguments") or {}  
        if name == "echo":  
            return {  
                "content": [{"type": "text", "text": str(arguments.get("text", ""))}],  
                "isError": False,  
            }  
        if name == "add":  
            total = arguments.get("a", 0) + arguments.get("b", 0)  
            return {  
                "content": [{"type": "text", "text": str(total)}],  
                "isError": False,  
            }  
        raise ValueError(f"unknown tool: {name}")  
    if method in ("resources/list", "resources/read") and NO_RESOURCES:  
        raise MethodError(-32601, f"Method not found: {method}")  
    if method == "resources/list":  
        return {"resources": RESOURCES}  
    if method == "resources/read":  
        uri = params.get("uri")  
        if uri == "memory://greeting":  
            text = "Ola! Bem-vindo ao fake backend."  
        else:  
            text = f"Conteudo fake do recurso {uri}"  
        return {  
            "contents": [{"uri": uri, "mimeType": "text/plain", "text": text}],  
        }  
    if method in ("prompts/list", "prompts/get") and NO_PROMPTS:  
        raise MethodError(-32601, f"Method not found: {method}")  
    if method == "prompts/list":  
        return {"prompts": PROMPTS}  
    if method == "prompts/get":  
        name = params.get("name")  
        arguments = params.get("arguments") or {}  
        if name == "greet":  
            person = arguments.get("person") or "mundo"  
            return {  
                "description": "Uma saudacao.",  
                "messages": [  
                    {  
                        "role": "user",  
                        "content": {"type": "text", "text": f"Ola, {person}!"},  
                    }  
                ],  
            }  
        raise ValueError(f"unknown prompt: {name}")  
    raise ValueError(f"unknown method: {method}")  
  
  
def main() -> None:  
    """Loop de leitura do stdin: parseia cada linha, despacha e responde.  
  
    Linhas em branco e JSON malformado são ignorados; notificações (sem  
    ``id``) não geram resposta. ``MethodError`` vira erro JSON-RPC com o  
    código próprio; qualquer outra exceção vira INTERNAL_ERROR (-32603) em  
    vez de derrubar o processo.  
    """  
    for raw_line in sys.stdin:  
        line = raw_line.strip()  
        if not line:  
            continue  
        try:  
            request = json.loads(line)  
        except json.JSONDecodeError:  
            continue  
        if request.get("id") is None:  
            continue  
        try:  
            result = handle_request(request)  
        except MethodError as exc:  
            response = {  
                "jsonrpc": "2.0",  
                "id": request.get("id"),  
                "error": {"code": exc.code, "message": exc.message},  
            }  
        except Exception as exc:  
            response = {  
                "jsonrpc": "2.0",  
                "id": request.get("id"),  
                "error": {"code": -32603, "message": str(exc)},  
            }  
        else:  
            response = {"jsonrpc": "2.0", "id": request.get("id"), "result": result}  
        print(json.dumps(response), flush=True)  
  
  
if __name__ == "__main__":  
    main()