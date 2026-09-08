"""Lógica MCP compartilhada pelos backends fake (stdio, HTTP e SSE).

Dados fixos (tools/resources/prompts) e o despacho JSON-RPC — a única coisa
que muda entre os fakes é o transporte, então a semântica do protocolo vive
aqui para os três serem o MESMO backend por trás (valida agregação/roteamento
equivalentes entre transportes nos testes de integração).
"""

import sys
from typing import Any

PROTOCOL_VERSION = "2024-11-05"

NO_RESOURCES = "--no-resources" in sys.argv
NO_PROMPTS = "--no-prompts" in sys.argv
RESPONSE_DELAY_SECONDS = (
    float(sys.argv[sys.argv.index("--delay") + 1]) if "--delay" in sys.argv else 0.0
)

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


def handle_request(request: dict[str, Any]) -> Any:
    """Despacha um request JSON-RPC e devolve o campo ``result``.

    Levanta MethodError para erros com código próprio (MethodNotFound de
    recursos não suportados) e ValueError para os demais — o transporte decide
    como virar resposta de erro.

    Flags:
        --no-resources: resources/list e resources/read respondem MethodNotFound
        --no-prompts: prompts/list e prompts/get respondem MethodNotFound
        --delay: atraso fixo (s) aplicado a cada resposta, exceto ping
    """
    method = request.get("method")
    params = request.get("params") or {}
    if RESPONSE_DELAY_SECONDS > 0 and method != "ping":
        # Atraso simulado (testes de timeout); ping nunca atrasa (health check).
        import time

        time.sleep(RESPONSE_DELAY_SECONDS)
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


def build_response(request: dict[str, Any]) -> dict[str, Any]:
    """Monta a resposta JSON-RPC completa para um request (result ou error)."""
    request_id = request.get("id")
    try:
        result = handle_request(request)
    except MethodError as exc:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": exc.code, "message": exc.message},
        }
    except Exception as exc:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32603, "message": str(exc)},
        }
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def is_notification(request: dict[str, Any]) -> bool:
    """Notificação JSON-RPC não tem id e não gera resposta."""
    return request.get("id") is None
