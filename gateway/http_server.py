"""HttpServer: POST /mcp, rotas de observabilidade/controle e dashboard.

Camada de transporte: cuida de autenticação, Content-Type, tamanho do payload
e parse do JSON. Tudo que é semântica do protocolo JSON-RPC é delegado ao
McpServer (camada de protocolo) — o HttpServer nunca fala direto com um Client;
rotas de controle falam com o BackendManager via McpServer.

Política de autenticação:
- ``GET /health``: NUNCA exige auth (mesmo com auth_token configurado) — é o
  endpoint para checagens de infraestrutura (load balancer, uptime monitor),
  que não têm como declarar o Bearer. Expõe só agregados, sem detalhes de
  comando/args.
- ``GET /api/servers``, ``POST /api/servers/{name}/disable|enable|restart`` e
  ``GET /api/tools/size``: autenticação SÓ via header Bearer (sem ``?token=``
  nessas rotas).
- ``POST /mcp`` e dashboard ``GET /``: aceitam header Bearer OU query string
  ``?token=<auth_token>``. Basta uma das formas estar correta (se ambas forem
  enviadas e só uma bater, o acesso é aceito — a forma correta já prova
  conhecimento do token). O header é a forma primária/recomendada; a query
  existe porque o Windows/cmd.exe corrompe headers com espaço em argumentos
  de ``npx`` (mcp-remote). O valor da query nunca aparece em logs: nenhum
  ponto emite a URL da request e o access log do uvicorn fica desligado.
"""

import html
import json
import secrets
import time
import uuid
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from gateway.config import DEFAULT_MAX_PAYLOAD_BYTES
from gateway.errors import (
    BackendError,
    BackendNotFoundError,
    BackendStateConflictError,
)
from gateway.models import INVALID_REQUEST, INTERNAL_ERROR, PARSE_ERROR, make_error
from gateway.server import McpServer
from gateway import __version__

logger = structlog.get_logger(__name__)

APP_VERSION = __version__

# Header de sessão da extensão de filtro seletivo (Fase 5) — definido em
# gateway.sessions e reexportado aqui para uso das rotas.
from gateway.sessions import SESSION_HEADER  # noqa: E402


# ----------------------------------------------------------------------
# Dashboard read-only (Fase 4) — server-rendered, sem JavaScript
# ----------------------------------------------------------------------

_DASHBOARD_STYLE = """
body { font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a2e; }
h1 { font-size: 1.4rem; } h2 { font-size: 1.1rem; margin-top: 1.5rem; }
table { border-collapse: collapse; width: 100%; max-width: 60rem; }
th, td { border: 1px solid #d0d0dd; padding: 0.4rem 0.6rem; text-align: left; }
th { background: #eef0f8; }
.status-RUNNING { color: #1b7f3b; font-weight: 600; }
.status-OFFLINE, .status-FAILED { color: #b02a2a; font-weight: 600; }
.status-RESTARTING { color: #b26a00; font-weight: 600; }
.status-DISABLED { color: #5a5a6e; font-weight: 600; }
.ok { color: #1b7f3b; font-weight: 600; } .degraded { color: #b26a00; font-weight: 600; }
"""


def _render_dashboard(summary: dict[str, Any], servers: list[dict[str, Any]]) -> str:
    """Gera o HTML do dashboard (read-only, sem JavaScript).

    Todo valor dinâmico passa por ``html.escape`` — os campos vêm do config/
    dos backends (nomes, urls, comandos) e nunca podem injetar marcação.
    """
    status = html.escape(str(summary.get("status", "?")))
    backends_summary = summary.get("backends", {})
    totals = summary.get("tools_count", 0)
    res_count = summary.get("resources_count", 0)
    prompt_count = summary.get("prompts_count", 0)
    rows: list[str] = []
    for server in servers:
        status_value = str(server.get("status", ""))
        status_cell = html.escape(status_value)
        cells = (
            html.escape(str(server.get("name", ""))),
            html.escape(str(server.get("type", ""))),
            status_cell,
            str(server.get("tools_count", 0)),
            str(server.get("resources_count", 0)),
            str(server.get("prompts_count", 0)),
            str(server.get("consecutive_failures", 0)),
            html.escape(str(server.get("url") or "")),
            html.escape(
                " ".join([str(server.get("command") or ""), *server.get("args", [])]).strip()
            ),
        )
        row_cells = "".join(
            f'<td class="status-{status_cell.upper()}">{cells[2]}</td>'
            if i == 2
            else f"<td>{cell}</td>"
            for i, cell in enumerate(cells)
        )
        rows.append(f"<tr>{row_cells}</tr>")
    counters = "".join(
        f"<li>{html.escape(str(key))}: {value}</li>"
        for key, value in backends_summary.items()
    )
    rows_html = "\n".join(rows)
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>MCP Gateway — Dashboard</title>
<style>{_DASHBOARD_STYLE}</style>
</head>
<body>
<h1>MCP Gateway</h1>
<p>Status geral: <span class="{status}">{status}</span></p>
<p>Tools agregadas: {totals} · Resources: {res_count} · Prompts: {prompt_count}</p>
<h2>Backends</h2>
<ul>{counters}</ul>
<table>
<thead><tr><th>Nome</th><th>Tipo</th><th>Status</th><th>Tools</th><th>Resources</th>
<th>Prompts</th><th>Falhas consecutivas</th><th>URL</th><th>Comando</th></tr></thead>
<tbody>
{rows_html}
</tbody>
</table>
<p><em>Read-only — ações de controle via API (ver README).</em></p>
</body>
</html>"""


def create_app(
    mcp_server: McpServer,
    *,
    auth_token: str | None = None,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
) -> FastAPI:
    """Cria a aplicação FastAPI com POST /mcp, GET /health, GET /api/servers,
    rotas de controle dos backends (Fase 4) e dashboard GET /.

    Validações de transporte do POST /mcp, nesta ordem:
    1. autenticação — header ``Authorization: Bearer <token>`` ou query
       ``?token=<token>`` (só quando ``auth_token`` é configurado) -> 401;
    2. Content-Type deve ser application/json -> 415;
    3. payload não pode exceder ``max_payload_bytes`` -> 413;
    4. body deve ser JSON válido -> ParseError (-32700).

    Cada request HTTP ganha um ``request_id`` (UUID) vinculado via
    contextvars; todos os logs da mesma requisição carregam o mesmo id
    automaticamente (ver gateway.logging).
    """
    app = FastAPI(
        title="MCP Gateway",
        version=APP_VERSION,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    def _is_authorized(request: Request, token_override: str | None = None) -> bool:
        """Checa o token configurado contra o header Bearer e/ou ``?token=``.

        O token pode chegar de duas formas, nesta ordem de checagem:
        1. header ``Authorization: Bearer <token>`` (forma primária);
        2. query string ``?token=<token>`` (``token_override`` — usado pelo
           dashboard e, desde a correção de escaping do Windows, também pelo
           POST /mcp: ``cmd.exe`` corrompe headers com espaço em argumentos
           de ``npx``).

        Basta UMA das formas bater com o token configurado: se ambas forem
        enviadas e só uma estiver correta, o acesso é aceito (a forma correta
        já prova conhecimento do token — exigir as duas não adicionaria
        segurança, só fragilidade). O header é checado primeiro, mas um Bearer
        errado NÃO invalida um token de query correto. Comparação sempre em
        tempo constante (``secrets.compare_digest``).
        """
        if auth_token is None:
            return True
        header = request.headers.get("authorization", "")
        scheme, _, token = header.partition(" ")
        if scheme.lower() == "bearer" and bool(token):
            if secrets.compare_digest(token, auth_token):
                return True
        if token_override is not None:
            return secrets.compare_digest(token_override, auth_token)
        return False

    def _unauthorized() -> JSONResponse:
        return JSONResponse(
            status_code=401,
            content={"detail": "Autenticação necessária: header 'Authorization: Bearer <token>'"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    def _backend_error_response(exc: BackendError) -> JSONResponse:
        """Traduz BackendError das rotas de controle em 404/409/503.

        BackendNotFoundError -> 404; BackendStateConflictError -> 409;
        demais (falha de subida, indisponibilidade) -> 503.
        """
        message = str(exc)
        if isinstance(exc, BackendNotFoundError):
            status = 404
        elif isinstance(exc, BackendStateConflictError):
            status = 409
        else:
            status = 503
        return JSONResponse(status_code=status, content={"detail": message})

    # ------------------------------------------------------------------
    # Rotas de observabilidade (Fase 2) e dashboard (Fase 4)
    # ------------------------------------------------------------------

    @app.get("/health")
    async def health() -> JSONResponse:
        """Status geral do Gateway; sem auth por design (checagens de infra)."""
        return JSONResponse(content=mcp_server.backend_manager.health_summary())

    @app.get("/api/servers")
    async def servers(request: Request) -> JSONResponse:
        """Detalhe operacional de cada backend; mesma auth do POST /mcp."""
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/servers")
            return _unauthorized()
        return JSONResponse(content={"servers": mcp_server.backend_manager.server_details()})

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request) -> Response:
        """Dashboard read-only (server-rendered, sem JavaScript).

        MESMA auth do /api/servers: expõe detalhes operacionais (comandos,
        urls). Como navegador não declara Bearer, aceita ``/?token=<token>``
        quando auth está configurada — o header, quando presente, vence.
        """
        query_token = request.query_params.get("token")
        if not _is_authorized(request, token_override=query_token):
            logger.info("http_auth_rejected", route="/")
            if auth_token is None:
                return _unauthorized()  # inalcançável; guarda de contrato
            return HTMLResponse(
                status_code=401,
                content=(
                    "<!DOCTYPE html><html lang='pt-BR'><head><meta charset='utf-8'>"
                    "<title>401</title></head><body><h1>401 — Autenticação necessária</h1>"
                    "<p>Este dashboard é protegido pelo mesmo token do Gateway.</p>"
                    "<p>Acesse <code>/?token=SEU_TOKEN</code> (o token está em"
                    " <code>auth_token</code> no config.json).</p></body></html>"
                ),
            )
        return HTMLResponse(
            content=_render_dashboard(
                mcp_server.backend_manager.health_summary(),
                mcp_server.backend_manager.server_details(),
            )
        )

    # ------------------------------------------------------------------
    # Rotas de controle manual dos backends (Fase 4)
    # ------------------------------------------------------------------

    async def _control_route(
        request: Request, name: str, action: str, operation: Any
    ) -> JSONResponse:
        """Esqueleto comum das rotas disable/enable/restart.

        ``operation`` é um callable SEM argumentos (ex.: ``lambda:
        manager.disable(name)``) — e não um coroutine pronto: assim o coroutine
        só é criado DEPOIS da checagem de auth (criá-lo antes deixaria um
        "coroutine was never awaited" a cada 401).

        Auth igual ao /mcp; BackendError vira 404 (inexistente) / 409 (estado
        conflitante) / 503 (operação falhou) — nunca um 500 genérico. Cada
        resposta inclui o estado atualizado do backend (o cliente vê o
        resultado sem precisar de segunda chamada).
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route=f"/api/servers/{name}/{action}")
            return _unauthorized()
        try:
            await operation()
        except BackendError as exc:
            return _backend_error_response(exc)
        state = mcp_server.backend_manager.get_state(name)
        assert state is not None  # noqa: S101 — coro succeeded ⇒ backend existe
        return JSONResponse(
            content={
                "backend": name,
                "action": action,
                "status": state.status.value,
            }
        )

    @app.post("/api/servers/{name}/disable")
    async def disable_backend(name: str, request: Request) -> JSONResponse:
        """Desliga um backend intencionalmente (estado 'disabled', sem auto-restart)."""
        return await _control_route(
            request, name, "disable", lambda: mcp_server.backend_manager.disable(name)
        )

    @app.post("/api/servers/{name}/enable")
    async def enable_backend(name: str, request: Request) -> JSONResponse:
        """Reverte 'disabled': sobe o backend e reintegra nos registries."""
        return await _control_route(
            request, name, "enable", lambda: mcp_server.backend_manager.enable(name)
        )

    @app.post("/api/servers/{name}/restart")
    async def restart_backend(name: str, request: Request) -> JSONResponse:
        """Restart manual imediato — funciona inclusive com o backend running."""
        return await _control_route(
            request, name, "restart", lambda: mcp_server.backend_manager.restart(name)
        )

    @app.get("/api/tools/size")
    async def tools_size(request: Request) -> JSONResponse:
        """Diagnóstico (Fase 5): tamanho do tools/list (chars/tokens aprox.).

        Mesma auth do /api/servers: os payloads das tools podem revelar
        detalhes da superfície exposta. Aceita ``Mcp-Session-Id`` para medir o
        tools/list DE UMA SESSÃO filtrada — é o que o operador usa para decidir
        se o filtro seletivo vale a pena (e quanto economiza).
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/tools/size")
            return _unauthorized()
        return JSONResponse(
            content=mcp_server.tools_list_size(request.headers.get(SESSION_HEADER))
        )

    # ------------------------------------------------------------------
    # POST /mcp
    # ------------------------------------------------------------------

    async def _process_request(request: Request) -> Response:
        # 1. Autenticação (opcional; se configurada, exigida em toda chamada).
        #    Aceita Bearer no header OU ``?token=`` na query (o Windows/cmd.exe
        #    corrompe headers com espaço em argumentos de npx — ver README).
        #    O valor da query NUNCA é logado: nenhum ponto deste módulo emite
        #    a URL da request, e o access log do uvicorn fica desligado em
        #    main.py (a observabilidade é o ``http_request_completed``, que
        #    não inclui URL).
        if not _is_authorized(request, token_override=request.query_params.get("token")):
            logger.info("http_auth_rejected")
            return JSONResponse(
                status_code=401,
                content={"detail": "Autenticação necessária: header 'Authorization: Bearer <token>'"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        # 2. Content-Type.
        content_type = request.headers.get("content-type", "")
        media_type = content_type.split(";", 1)[0].strip().lower()
        if media_type != "application/json":
            return JSONResponse(
                status_code=415,
                content={"detail": "Content-Type deve ser application/json"},
            )

        # 3. Tamanho do payload (limite vindo do config).
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                declared_length = int(content_length)
            except ValueError:
                declared_length = None
            if declared_length is not None and declared_length > max_payload_bytes:
                logger.warning("http_payload_too_large", max_bytes=max_payload_bytes)
                return JSONResponse(
                    status_code=413,
                    content={
                        "detail": f"Payload excede o limite de {max_payload_bytes} bytes"
                    },
                )

        body_parts: list[bytes] = []
        body_size = 0
        async for chunk in request.stream():
            body_size += len(chunk)
            if body_size > max_payload_bytes:
                logger.warning("http_payload_too_large", max_bytes=max_payload_bytes)
                return JSONResponse(
                    status_code=413,
                    content={
                        "detail": f"Payload excede o limite de {max_payload_bytes} bytes"
                    },
                )
            body_parts.append(chunk)
        body = b"".join(body_parts)
        if body_size > max_payload_bytes:
            logger.warning("http_payload_too_large", max_bytes=max_payload_bytes)
            return JSONResponse(
                status_code=413,
                content={"detail": f"Payload excede o limite de {max_payload_bytes} bytes"},
            )

        # 4. Parse do JSON -> ParseError se malformado.
        try:
            raw_body: Any = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return JSONResponse(
                status_code=400,
                content=make_error(None, PARSE_ERROR, "Parse error"),
            )

        if not isinstance(raw_body, dict):
            # Batch não é suportado nesta fase; resposta JSON-RPC no corpo.
            return JSONResponse(
                status_code=200,
                content=make_error(
                    None,
                    INVALID_REQUEST,
                    "Invalid Request: body deve ser um objeto JSON-RPC único (batch não suportado)",
                ),
            )

        try:
            response = await mcp_server.process_message(
                raw_body, session_id=request.headers.get(SESSION_HEADER)
            )
        except Exception as exc:
            logger.exception("erro inesperado processando mensagem JSON-RPC", error=str(exc))
            return JSONResponse(
                status_code=200,
                content=make_error(None, INTERNAL_ERROR, "Internal error"),
            )
        if response is None:
            return Response(status_code=202)  # notificação: sem corpo
        return JSONResponse(status_code=200, content=response)

    @app.post("/mcp")
    async def mcp_endpoint(request: Request) -> Response:
        request_id = uuid.uuid4().hex
        structlog.contextvars.bind_contextvars(request_id=request_id)
        started = time.perf_counter()
        try:
            response = await _process_request(request)
            duration_ms = round((time.perf_counter() - started) * 1000, 1)
            logger.info(
                "http_request_completed",
                status_code=response.status_code,
                duration_ms=duration_ms,
            )
            return response
        except Exception:
            logger.exception("erro inesperado no endpoint /mcp")
            return JSONResponse(status_code=500, content={"detail": "Internal error"})
        finally:
            structlog.contextvars.clear_contextvars()

    return app
