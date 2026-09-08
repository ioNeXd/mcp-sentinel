"""McpServer: protocolo JSON-RPC do Gateway (agregação de backends).

Camada de protocolo: recebe o envelope JSON-RPC já parseado (dict), valida o
envelope, despacha para os handlers de tools/resources/prompts e devolve a
resposta JSON-RPC. Nunca faz I/O direto além das chamadas aos backends — e o
acesso aos clients é sempre via BackendManager (camada de roteamento), nunca
direto a um Client (regra de camadas do AGENT_INSTRUCTIONS).

Extensão de filtro seletivo por sessão (Fase 5, NÃO faz parte do spec MCP):
``gateway/session/set_active_backends``, ``gateway/session/get_active_backends``
e ``gateway/session/clear_active_backends`` — a sessão é identificada pelo
header HTTP ``Mcp-Session-Id``. Sessão sem filtro vê TUDO (compatível com
clientes que não conhecem a extensão); o filtro é uma VIEW calculada na hora
de responder — os registries globais continuam sendo a fonte única de verdade.
"""

import json
import time
from typing import Any

import structlog
from pydantic import ValidationError

from gateway.backend_manager import BackendManager
from gateway.errors import BackendError, BackendJsonRpcError
from gateway.models import (
    BACKEND_UNAVAILABLE,
    INVALID_PARAMS,
    INVALID_REQUEST,
    ITEM_NOT_FOUND,
    METHOD_NOT_FOUND,
    JsonRpcRequest,
    make_error,
    make_result,
)
from gateway.registries import PromptRegistry, ResourceRegistry, ToolRegistry
from gateway.registries.base import RegistryEntry
from gateway.sessions import SESSION_HEADER, SessionFilter
from gateway.version import __version__

logger = structlog.get_logger(__name__)

PROTOCOL_VERSION = "2024-11-05"
SERVER_NAME = "mcp-gateway"
SERVER_VERSION = __version__

# Divisor heurístico para estimar tokens a partir de caracteres JSON (~4 chars
# por token em inglês/JSON). Estimativa GROSSEIRA de diagnóstico — não substitui
# um tokenizer de verdade (ver GET /api/tools/size no README).
APPROX_CHARS_PER_TOKEN = 4


class McpServer:
    """Agrega N backends MCP atrás de um único endpoint JSON-RPC.

    Tools, resources e prompts de cada backend são expostos com namespace
    ``backend.<id original>``; o roteamento de ``tools/call``,
    ``resources/read`` e ``prompts/get`` devolve cada chamada ao backend
    correto com o identificador original.

    Attributes:
        backend_manager: Dono do ciclo de vida dos backends; usado para
            iniciar/parar tudo e para resolver o client ativo de cada
            chamada (reflete restarts sem precisar de reinício do Gateway).
        registries: Registries agregados mantidos em sincronia com os
            backends vivos pelo BackendManager.
        sessions: Filtro seletivo de backends por sessão (Fase 5). Sessão sem
            filtro vê tudo — o comportamento default é idêntico ao das fases
            anteriores.
    """

    def __init__(
        self,
        backend_manager: BackendManager,
        registries: tuple[ToolRegistry, ResourceRegistry, PromptRegistry],
        session_filter: SessionFilter | None = None,
    ) -> None:
        self.backend_manager = backend_manager
        self.registries = registries
        self._tools, self._resources, self._prompts = registries
        # Default cria o próprio filtro (TTL padrão do config não chega aqui
        # porque main.py sempre injeta; construtores de teste ficam simples).
        self.sessions = session_filter if session_filter is not None else SessionFilter(3600.0)

    async def start(self) -> None:
        """Sobe todos os backends via BackendManager (handshake + registries)."""
        await self.backend_manager.start_all()
        logger.info(
            "gateway_ready",
            backends=len(self.backend_manager.all_states()),
            tools=len(self._tools.list_all()),
            resources=len(self._resources.list_all()),
            prompts=len(self._prompts.list_all()),
        )

    async def stop(self) -> None:
        """Encerra todos os backends via BackendManager (idempotente)."""
        await self.backend_manager.stop_all()

    # ------------------------------------------------------------------
    # Processamento do envelope JSON-RPC
    # ------------------------------------------------------------------

    async def process_message(
        self, raw_body: dict[str, Any], session_id: str | None = None
    ) -> dict[str, Any] | None:
        """Processa um request JSON-RPC e devolve a resposta (None p/ notificação).

        ``session_id`` vem do header ``Mcp-Session-Id`` (ver SESSION_HEADER);
        sem header, ``None`` — e sem sessão não há filtro (comportamento
        default, compatível com qualquer cliente MCP).

        Tabela de erros (documentada no README):
        - Envelope malformado/campos obrigatórios ausentes -> InvalidRequest (-32600)
          (ausência de ``method``/``jsonrpc`` é Request inválido, não InvalidParams
          — este fica reservado para parâmetros inválidos de um método conhecido);
        - Method desconhecido -> MethodNotFound (-32601);
        - tool/resource/prompt namespaced inexistente -> ITEM_NOT_FOUND (-32001);
        - falha do backend durante a chamada -> BACKEND_UNAVAILABLE (-32002);
        - erros JSON-RPC vindos do backend são repassados com o código original.
        """
        started = time.perf_counter()
        request_id = self._extract_id(raw_body)
        envelope_error = self._envelope_error(raw_body, request_id)
        if envelope_error is not None:
            logger.warning("jsonrpc_invalid_request", id=request_id, reason=envelope_error)
            return make_error(request_id, INVALID_REQUEST, f"Invalid Request: {envelope_error}")
        try:
            request = JsonRpcRequest.model_validate(raw_body)
        except ValidationError as exc:
            # Envelope nominalmente presente, mas com tipo inválido em algum campo.
            logger.warning("jsonrpc_invalid_request", id=request_id, reason=str(exc))
            return make_error(request_id, INVALID_REQUEST, "Invalid Request", data=str(exc))
        if request.id is None:
            return None  # notificação: o protocolo não exige resposta
        logger.info(
            "jsonrpc_request_received", method=request.method, id=request.id, session_id=session_id
        )
        response = await self._dispatch(request, session_id)
        if response is None:
            return None
        duration_ms = round((time.perf_counter() - started) * 1000, 1)
        error = response.get("error")
        if error is not None:
            logger.warning(
                "jsonrpc_request_error",
                method=request.method,
                id=request.id,
                code=error.get("code"),
                duration_ms=duration_ms,
            )
        else:
            logger.info(
                "jsonrpc_request_ok",
                method=request.method,
                id=request.id,
                duration_ms=duration_ms,
            )
        return response

    async def _dispatch(
        self, request: JsonRpcRequest, session_id: str | None = None
    ) -> dict[str, Any] | None:
        if request.method == "initialize":
            return make_result(
                request.id,
                {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {
                        "tools": {"listChanged": False},
                        "resources": {"subscribe": False, "listChanged": False},
                        "prompts": {"listChanged": False},
                    },
                    "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                },
            )
        if request.method == "ping":
            return make_result(request.id, {})
        if request.method == "gateway/session/set_active_backends":
            return self._handle_set_active_backends(request, session_id)
        if request.method == "gateway/session/get_active_backends":
            return self._handle_get_active_backends(request, session_id)
        if request.method == "gateway/session/clear_active_backends":
            return self._handle_clear_active_backends(request, session_id)
        if request.method == "tools/list":
            return make_result(
                request.id,
                {
                    "tools": [
                        self._tool_payload(entry)
                        for entry in self._visible(self._tools.list_all(), session_id)
                    ]
                },
            )
        if request.method == "resources/list":
            return make_result(
                request.id,
                {
                    "resources": [
                        self._resource_payload(entry)
                        for entry in self._visible(self._resources.list_all(), session_id)
                    ]
                },
            )
        if request.method == "prompts/list":
            return make_result(
                request.id,
                {
                    "prompts": [
                        self._prompt_payload(entry)
                        for entry in self._visible(self._prompts.list_all(), session_id)
                    ]
                },
            )
        if request.method == "tools/call":
            return await self._handle_tools_call(request, session_id)
        if request.method == "resources/read":
            return await self._handle_resource_read(request, session_id)
        if request.method == "prompts/get":
            return await self._handle_prompt_get(request, session_id)
        return make_error(request.id, METHOD_NOT_FOUND, f"Method not found: {request.method}")

    # ------------------------------------------------------------------
    # Filtro seletivo por sessão (Fase 5)
    # ------------------------------------------------------------------

    def _visible(
        self, entries: list[RegistryEntry], session_id: str | None
    ) -> list[RegistryEntry]:
        """Entra do registry filtrado pela sessão (view, sem copiar dados).

        Sem sessão ou sem filtro → lista completa (idêntico ao comportamento
        das fases anteriores). O acesso renova o TTL da sessão.
        """
        allowed = self.sessions.active_backends(session_id)
        if allowed is None:
            return entries
        return [entry for entry in entries if entry.backend in allowed]

    def _backend_allowed(self, backend: str, session_id: str | None) -> bool:
        """True se a sessão pode chamar itens deste backend (None = sem filtro)."""
        allowed = self.sessions.active_backends(session_id)
        return allowed is None or backend in allowed

    def _handle_set_active_backends(
        self, request: JsonRpcRequest, session_id: str | None
    ) -> dict[str, Any]:
        """Extensão: define o subconjunto de backends visível à sessão.

        Params: ``{"backends": ["backend-a", ...]}`` (lista não vazia de nomes
        existentes no config). Erro InvalidParams (-32602) se malformado ou se
        citar backend desconhecido — falha explícita, nunca filtro parcial
        silencioso.
        """
        if session_id is None:
            return make_error(
                request.id,
                INVALID_REQUEST,
                f"Invalid Request: extensão de sessão exige o header {SESSION_HEADER}",
            )
        params = request.params if isinstance(request.params, dict) else {}
        raw = params.get("backends")
        if (
            not isinstance(raw, list)
            or not raw
            or not all(isinstance(name, str) and name for name in raw)
        ):
            return make_error(
                request.id,
                INVALID_PARAMS,
                "Invalid params: 'backends' deve ser uma lista não vazia de nomes de backend",
            )
        known = set(self.backend_manager.all_states())
        unknown = sorted(set(raw) - known)
        if unknown:
            return make_error(
                request.id,
                INVALID_PARAMS,
                f"Invalid params: backends desconhecidos: {', '.join(unknown)}",
                data={"known_backends": sorted(known)},
            )
        self.sessions.set_active_backends(session_id, frozenset(raw))
        return make_result(request.id, {"active_backends": sorted(set(raw))})

    def _handle_get_active_backends(
        self, request: JsonRpcRequest, session_id: str | None
    ) -> dict[str, Any]:
        """Extensão: lista os backends ativos da sessão (null = sem filtro)."""
        active = self.sessions.active_backends(session_id)
        return make_result(
            request.id,
            {
                "active_backends": sorted(active) if active is not None else None,
                "filtered": active is not None,
            },
        )

    def _handle_clear_active_backends(
        self, request: JsonRpcRequest, session_id: str | None
    ) -> dict[str, Any]:
        """Extensão: remove o filtro — a sessão volta a ver todos os backends."""
        if session_id is not None:
            self.sessions.clear(session_id)
        return make_result(request.id, {"active_backends": None, "filtered": False})

    # ------------------------------------------------------------------
    # Handlers de chamada (roteamento por namespace)
    # ------------------------------------------------------------------

    async def _handle_tools_call(
        self, request: JsonRpcRequest, session_id: str | None = None
    ) -> dict[str, Any]:
        params = request.params if isinstance(request.params, dict) else {}
        tool_name = params.get("name")
        if not isinstance(tool_name, str) or not tool_name:
            return make_error(request.id, INVALID_PARAMS, "Invalid params: 'name' (string) é obrigatório")
        arguments = params.get("arguments")
        if arguments is not None and not isinstance(arguments, dict):
            return make_error(request.id, INVALID_PARAMS, "Invalid params: 'arguments' deve ser um objeto")
        entry = self._tools.get(tool_name)
        if entry is None:
            return make_error(request.id, ITEM_NOT_FOUND, f"Unknown tool: {tool_name}")
        if not self._backend_allowed(entry.backend, session_id):
            # Fora do filtro da sessão = não existe (mesmo código/mensagem de
            # uma tool inexistente — o filtro não vaza a existência bloqueada).
            logger.info(
                "request_blocked_by_session_filter",
                method="tools/call",
                item=entry.namespaced,
                session_id=session_id,
            )
            return make_error(request.id, ITEM_NOT_FOUND, f"Unknown tool: {tool_name}")
        logger.info("request_dispatched", method="tools/call", item=entry.namespaced, backend=entry.backend)
        return await self._call_backend(
            request.id, entry, "tools/call", {"name": entry.name, "arguments": arguments or {}}
        )

    async def _handle_resource_read(
        self, request: JsonRpcRequest, session_id: str | None = None
    ) -> dict[str, Any]:
        params = request.params if isinstance(request.params, dict) else {}
        uri = params.get("uri")
        if not isinstance(uri, str) or not uri:
            return make_error(request.id, INVALID_PARAMS, "Invalid params: 'uri' (string) é obrigatório")
        entry = self._resources.get(uri)
        if entry is None:
            return make_error(request.id, ITEM_NOT_FOUND, f"Unknown resource: {uri}")
        if not self._backend_allowed(entry.backend, session_id):
            logger.info(
                "request_blocked_by_session_filter",
                method="resources/read",
                item=entry.namespaced,
                session_id=session_id,
            )
            return make_error(request.id, ITEM_NOT_FOUND, f"Unknown resource: {uri}")
        logger.info("request_dispatched", method="resources/read", item=entry.namespaced, backend=entry.backend)
        result = await self._call_backend(
            request.id, entry, "resources/read", {"uri": entry.name}
        )
        # resources/read devolve contents com a uri ORIGINAL do backend; reescrevê-la
        # para o formato namespaced mantém o round-trip (o cliente devolve a uri que
        # o Gateway anunciou no resources/list).
        if result.get("error") is None and isinstance(result.get("result"), dict):
            result = dict(result)
            result["result"] = self._namespace_content_uris(result["result"], entry.backend)
        return result

    async def _handle_prompt_get(
        self, request: JsonRpcRequest, session_id: str | None = None
    ) -> dict[str, Any]:
        params = request.params if isinstance(request.params, dict) else {}
        prompt_name = params.get("name")
        if not isinstance(prompt_name, str) or not prompt_name:
            return make_error(request.id, INVALID_PARAMS, "Invalid params: 'name' (string) é obrigatório")
        arguments = params.get("arguments")
        if arguments is not None and not isinstance(arguments, dict):
            return make_error(request.id, INVALID_PARAMS, "Invalid params: 'arguments' deve ser um objeto")
        entry = self._prompts.get(prompt_name)
        if entry is None:
            return make_error(request.id, ITEM_NOT_FOUND, f"Unknown prompt: {prompt_name}")
        if not self._backend_allowed(entry.backend, session_id):
            logger.info(
                "request_blocked_by_session_filter",
                method="prompts/get",
                item=entry.namespaced,
                session_id=session_id,
            )
            return make_error(request.id, ITEM_NOT_FOUND, f"Unknown prompt: {prompt_name}")
        logger.info("request_dispatched", method="prompts/get", item=entry.namespaced, backend=entry.backend)
        call_params: dict[str, Any] = {"name": entry.name}
        if arguments is not None:
            call_params["arguments"] = arguments
        return await self._call_backend(request.id, entry, "prompts/get", call_params)

    async def _call_backend(
        self,
        request_id: int | str | None,
        entry: RegistryEntry,
        method: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Encaminha a chamada ao backend e traduz falhas em erro JSON-RPC.

        O client ativo é resolvido no BackendManager a cada chamada — se o
        backend estiver offline/restartando, a chamada vira
        BACKEND_UNAVAILABLE sem afetar o Gateway. Erros JSON-RPC do backend
        são repassados com o código original; falhas de transporte (timeout,
        processo morto etc.) viram BACKEND_UNAVAILABLE.
        """
        try:
            client = self.backend_manager.get_client(entry.backend)
            result = await client.send_request(method, params)
        except BackendJsonRpcError as exc:
            logger.warning(
                "backend_jsonrpc_error",
                backend=entry.backend,
                method=method,
                code=exc.code,
                message=exc.message,
            )
            return make_error(request_id, exc.code, exc.message, data=exc.data)
        except BackendError as exc:
            logger.warning(
                "backend_unavailable", backend=entry.backend, method=method, error=str(exc)
            )
            return make_error(
                request_id, BACKEND_UNAVAILABLE, f"Backend '{entry.backend}' indisponível: {exc}"
            )
        return make_result(request_id, result)

    # ------------------------------------------------------------------
    # Helpers de validação/serialização
    # ------------------------------------------------------------------

    def tools_list_size(self, session_id: str | None = None) -> dict[str, Any]:
        """Diagnóstico (Fase 5): tamanho do tools/list desta sessão/all.

        Estimativa SIMPLES por caracteres (``len(json.dumps)``) e tokens
        aproximados (chars/4, heurística grosseira — não é tokenizer real).
        Serve para decidir na prática se o filtro seletivo vale a pena para um
        config específico (ver GET /api/tools/size no README).
        """
        entries = self._visible(self._tools.list_all(), session_id)
        payloads = [self._tool_payload(entry) for entry in entries]
        serialized = json.dumps({"tools": payloads}, ensure_ascii=False)
        per_backend: dict[str, int] = {}
        for entry, payload in zip(entries, payloads):
            per_backend[entry.backend] = per_backend.get(entry.backend, 0) + len(
                json.dumps(payload, ensure_ascii=False)
            )
        filtered = self.sessions.active_backends(session_id) is not None
        return {
            "tools_count": len(entries),
            "json_chars": len(serialized),
            "approx_tokens": len(serialized) // APPROX_CHARS_PER_TOKEN,
            "per_backend_chars": dict(sorted(per_backend.items())),
            "session_id": session_id,
            "filtered": filtered,
        }

    @staticmethod
    def _extract_id(raw_body: dict[str, Any]) -> int | str | None:
        candidate = raw_body.get("id")
        if isinstance(candidate, bool):  # bool é subclass de int; não é id válido
            return None
        return candidate if isinstance(candidate, (int, str)) else None

    @staticmethod
    def _envelope_error(raw_body: dict[str, Any], request_id: int | str | None) -> str | None:
        """Devolve mensagem de erro do envelope JSON-RPC, ou None se válido."""
        if raw_body.get("id") is not None and request_id is None:
            return "'id' deve ser uma string ou número"
        if raw_body.get("jsonrpc") != "2.0":
            return "'jsonrpc' ausente ou inválido (deve ser \"2.0\")"
        method = raw_body.get("method")
        if not isinstance(method, str) or not method:
            return "'method' ausente ou inválido (deve ser string não vazia)"
        return None

    @staticmethod
    def _tool_payload(entry: RegistryEntry) -> dict[str, Any]:
        payload = dict(entry.metadata)
        payload["name"] = entry.namespaced
        return payload

    @staticmethod
    def _resource_payload(entry: RegistryEntry) -> dict[str, Any]:
        payload = dict(entry.metadata)
        payload["uri"] = entry.namespaced
        return payload

    @staticmethod
    def _prompt_payload(entry: RegistryEntry) -> dict[str, Any]:
        payload = dict(entry.metadata)
        payload["name"] = entry.namespaced
        return payload

    @staticmethod
    def _namespace_content_uris(result: dict[str, Any], backend: str) -> dict[str, Any]:
        contents = result.get("contents")
        if not isinstance(contents, list):
            return result
        rewritten = []
        for item in contents:
            if isinstance(item, dict) and isinstance(item.get("uri"), str):
                item = dict(item)
                item["uri"] = f"{backend}.{item['uri']}"
            rewritten.append(item)
        return {**result, "contents": rewritten}
