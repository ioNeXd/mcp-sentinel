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

import asyncio
import html
import json
import os
import re
import secrets
import threading
import time
import uuid
import webbrowser
from pathlib import Path
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pydantic import ValidationError

from gateway.config import (
    BACKEND_NAME_PATTERN,
    DEFAULT_MAX_PAYLOAD_BYTES,
    BackendConfig,
    GatewayConfig,
)
from gateway.errors import (
    BackendError,
    BackendNotFoundError,
    BackendStateConflictError,
)
from gateway.log_stream import log_broadcaster
from gateway.rate_limiter import RateLimiter
from gateway.models import INVALID_REQUEST, INTERNAL_ERROR, PARSE_ERROR, make_error
from gateway.server import McpServer
from gateway import __version__

from contextlib import asynccontextmanager  
from collections.abc import AsyncIterator

# Reexporta o header de sessão do filtro seletivo (Fase 5) definido em
# gateway.sessions, para uso das rotas deste módulo. Import ao final do bloco
# (E402) por convenção de agrupamento após os imports de terceiros/pacote.
from gateway.sessions import SESSION_HEADER, normalize_session_id  # noqa: E402

logger = structlog.get_logger(__name__)

APP_VERSION = __version__

# Rate limiter para /api/logs/stream (10 conn/min por IP)
_log_stream_limiter = RateLimiter(max_requests=10, window_seconds=60)

_BACKEND_NAME_RE = re.compile(BACKEND_NAME_PATTERN)
"""Regex compilado do padrão de nome de backend importado de ``gateway.config``
— fonte única de verdade. A validação da rota e o schema Pydantic de
``BackendConfig`` usam o MESMO padrão, então nunca divergem: um payload que
passa na rota jamais é rejeitado (ou gravado quebrando o boot) pelo schema.
Antes, a rota usava ``c.isalnum()``, que é Unicode-aware e aceitava acentos
(``café``) que o schema rejeita — o config.json ficava com um backend
inválido e o Gateway não subia no boot seguinte."""


_DASHBOARD_STYLE = """  
:root {  
  color-scheme: light dark;  
  --bg: #0f1117; --panel: #171a23; --panel-2: #1d212c; --border: #2a2f3d;  
  --text: #e6e8ef; --muted: #8b91a5; --accent: #5b8cff;  
  --ok: #34c77b; --warn: #e0a63a; --err: #e05a5a; --off: #6b7284;  
}  
* { box-sizing: border-box; }  
body {  
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;  
  margin: 0; background: var(--bg); color: var(--text);  
}  
header {  
  display: flex; align-items: center; gap: 1rem; flex-wrap: wrap;  
  padding: 1rem 1.5rem; border-bottom: 1px solid var(--border);  
  background: var(--bg);  
}  
header h1 { font-size: 1.15rem; margin: 0; font-weight: 600; }  
.pill {  
  display: inline-flex; align-items: center; gap: .4rem; padding: .2rem .6rem;  
  border-radius: 999px; font-size: .8rem; font-weight: 600; border: 1px solid var(--border);  
}  
.pill .dot { width: .5rem; height: .5rem; border-radius: 50%; }  
.pill-ok .dot { background: var(--ok); } .pill-ok { color: var(--ok); }  
.pill-degraded .dot { background: var(--warn); } .pill-degraded { color: var(--warn); }  
.stat { color: var(--muted); font-size: .82rem; }  
main { padding: 1.25rem 1.5rem 2rem; max-width: 78rem; margin: 0 auto; }  
h2 { font-size: .95rem; text-transform: uppercase; letter-spacing: .04em;  
  color: var(--muted); margin: 1.75rem 0 .75rem; }  
#backends-grid {  
  display: grid; grid-template-columns: repeat(auto-fill, minmax(17rem, 1fr));  
  gap: .8rem;  
}  
.card {  
  background: var(--panel); border: 1px solid var(--border); border-radius: .6rem;  
  padding: .9rem 1rem;  
}  
.card-head { display: flex; justify-content: space-between; align-items: center; gap: .5rem; }  
.card-head .name { font-weight: 600; }  
.badge {  
  font-size: .72rem; font-weight: 700; text-transform: uppercase; letter-spacing: .03em;  
  padding: .12rem .5rem; border-radius: .3rem;  
}  
.badge-running { background: rgba(52,199,123,.15); color: var(--ok); }  
.badge-offline, .badge-failed { background: rgba(224,90,90,.15); color: var(--err); }  
.badge-restarting { background: rgba(224,166,58,.15); color: var(--warn); }  
.badge-disabled { background: rgba(107,114,132,.2); color: var(--off); }  
.card .meta { color: var(--muted); font-size: .78rem; margin-top: .3rem; word-break: break-all; }  
.card .counts { display: flex; gap: .9rem; margin-top: .6rem; font-size: .8rem; }  
.card .counts b { color: var(--text); }  
.card .actions { display: flex; gap: .4rem; margin-top: .8rem; }  
button.act {  
  flex: 1; background: var(--panel-2); border: 1px solid var(--border); color: var(--text);  
  padding: .35rem .5rem; border-radius: .4rem; font-size: .76rem; cursor: pointer;  
}  
button.act:hover { border-color: var(--accent); }
button.act:disabled { opacity: .4; cursor: not-allowed; }

#console-wrap {  
  background: var(--panel); border: 1px solid var(--border); border-radius: .6rem;  
  overflow: hidden;  
}  
#console-toolbar {  
  display: flex; align-items: center; gap: .6rem; padding: .5rem .8rem;  
  border-bottom: 1px solid var(--border); font-size: .8rem; color: var(--muted);  
}  
#console-toolbar .grow { flex: 1; }  
#console-toolbar button {  
  background: var(--panel-2); border: 1px solid var(--border); color: var(--text);  
  padding: .25rem .6rem; border-radius: .35rem; font-size: .76rem; cursor: pointer;  
}  
#console {  
  height: 20rem; overflow-y: auto; padding: .6rem .8rem; font-size: .78rem;  
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;  
  white-space: pre-wrap; word-break: break-all;  
}  
#console .line { padding: .06rem 0; }  
#console .lvl-error, #console .lvl-critical { color: var(--err); }  
#console .lvl-warning { color: var(--warn); }  
#console .lvl-info { color: var(--text); }  
#console .lvl-debug { color: var(--muted); }  
#console .ts { color: var(--muted); margin-right: .5rem; }  
.empty { color: var(--muted); font-size: .82rem; padding: .5rem 0; }  
.grow { flex: 1; }  
.btn-primary {  
  background: var(--accent); border: 1px solid var(--accent); color: #fff;  
  padding: .4rem .8rem; border-radius: .4rem; font-size: .82rem; font-weight: 600;  
  cursor: pointer;  
}  
.btn-primary:hover { filter: brightness(1.1); }  
.overlay {  
  position: fixed; inset: 0; background: rgba(0,0,0,.55);  
  display: flex; align-items: center; justify-content: center; z-index: 20;  
}  
.overlay.hidden { display: none; }  
.modal {  
  background: var(--panel); border: 1px solid var(--border); border-radius: .7rem;  
  width: 26rem; max-width: 92vw; max-height: 88vh; overflow-y: auto;  
  padding: 1rem 1.2rem 1.2rem;  
}  
.modal-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: .4rem; }  
.modal-head h3 { margin: 0; font-size: 1rem; }  
.btn-icon {  
  background: none; border: none; color: var(--muted); font-size: 1.3rem;  
  cursor: pointer; line-height: 1;  
}  
.modal label {
  display: block; font-size: .8rem; color: var(--muted); margin-top: .7rem;
}
.modal input, .modal select, .modal textarea {
  width: 100%; margin-top: .25rem; background: var(--panel-2); border: 1px solid var(--border);
  color: var(--text); border-radius: .35rem; padding: .4rem .5rem; font-size: .85rem;
  font-family: inherit;
}
.modal textarea { font-family: ui-monospace, monospace; resize: vertical; }  
.type-fields.hidden { display: none; }  
.hint { color: var(--muted); font-size: .74rem; margin-top: .8rem; }  
.error { color: var(--err); font-size: .78rem; margin-top: .5rem; }  
.error.hidden { display: none; }  
.modal-actions { display: flex; justify-content: flex-end; gap: .5rem; margin-top: 1rem; }  
.modal-actions button { padding: .4rem .9rem; border-radius: .4rem; font-size: .82rem; cursor: pointer; }  
#cancel-add-mcp { background: var(--panel-2); border: 1px solid var(--border); color: var(--text); }
.checkbox-label { display: flex !important; align-items: center; gap: .5rem; flex-direction: row-reverse; justify-content: flex-end; }
.checkbox-label input { width: auto; margin: 0; }
.settings-divider { border-top: 1px solid var(--border); margin: 1.1rem 0; }
#backup-restore h4 { font-size: .82rem; color: var(--muted); text-transform: uppercase; letter-spacing: .03em; margin: 0 0 .6rem; }
#cancel-settings { background: var(--panel-2); border: 1px solid var(--border); color: var(--text); }  
  
.conn-banner {
  background: var(--err); color: #fff; text-align: center; font-size: .85rem;
  padding: .45rem; font-weight: 600;
}
.conn-banner.hidden { display: none; }

/* Cabecalho + banner de conexao colados num unico bloco sticky, nessa ordem
   (banner ABAIXO do header): os dois permanecem visiveis juntos ao rolar a
   pagina, e o banner nunca fica coberto pelo header nem flutua acima dele. */
#sticky-top { position: sticky; top: 0; z-index: 10; }

.btn-secondary {
  background: var(--panel-2); border: 1px solid var(--border); color: var(--text);
  padding: .4rem .7rem; border-radius: .4rem; font-size: .8rem; cursor: pointer;
}
.btn-secondary:hover { border-color: var(--accent); }
.btn-danger {
  background: rgba(224,90,90,.12); border: 1px solid var(--err); color: var(--err);
  padding: .4rem .7rem; border-radius: .4rem; font-size: .8rem; cursor: pointer; font-weight: 600;
}
.btn-danger:hover { background: rgba(224,90,90,.22); }

.switch { display: inline-flex; align-items: center; gap: .35rem; font-size: .8rem; color: var(--muted); cursor: pointer; }
.switch input { accent-color: var(--accent); }

body.readonly-mode .actions, body.readonly-mode #open-add-mcp,
body.readonly-mode .card-remove, body.readonly-mode #shutdown-gateway { display: none; }

.gw-endpoint {
  font-size: .74rem; color: var(--muted); background: var(--panel-2);
  border: 1px solid var(--border); border-radius: .35rem; padding: .25rem .6rem;
  font-family: ui-monospace, monospace; white-space: nowrap; overflow: hidden;
  text-overflow: ellipsis; max-width: 28rem;
}

.group-header {
  font-size: .78rem; font-weight: 700; text-transform: uppercase; letter-spacing: .04em;
  color: var(--muted); margin: 1rem 0 .4rem; grid-column: 1 / -1;
}
.group-header:first-child { margin-top: 0; }

#toast-container {
  position: fixed; bottom: 1rem; right: 1rem; display: flex; flex-direction: column;
  gap: .5rem; z-index: 30;
}
.toast {
  background: var(--panel); border: 1px solid var(--border); border-left: 3px solid var(--accent);
  border-radius: .4rem; padding: .55rem .8rem; font-size: .8rem; color: var(--text);
  opacity: 0; transform: translateX(1rem); transition: opacity .2s, transform .2s;
  box-shadow: 0 4px 12px rgba(0,0,0,.3); max-width: 20rem;
}
.toast.show { opacity: 1; transform: translateX(0); }
.toast.toast-ok { border-left-color: var(--ok); }
.toast.toast-warn { border-left-color: var(--warn); }
.toast.toast-err { border-left-color: var(--err); }

.error-rate {
  font-size: .74rem; padding: .1rem .5rem; border-radius: .3rem;
  background: rgba(224,90,90,.15); color: var(--err); font-weight: 600;
}
.error-rate.hidden { display: none; }
#error-sparkline { vertical-align: middle; margin-right: .3rem; }
#console-level, #console-search {
  background: var(--panel-2); border: 1px solid var(--border); color: var(--text);
  font-size: .76rem; border-radius: .35rem; padding: .2rem .4rem;
}
#console-search { width: 10rem; }
#console-clear-filter.hidden { display: none; }
#console .line { cursor: pointer; display: flex; align-items: baseline; gap: .4rem; }
#console .line:hover { background: rgba(255,255,255,.04); }
#console .line.req-active { background: rgba(91,140,255,.12); }
#console .reqdot { width: .5rem; height: .5rem; border-radius: 50%; flex-shrink: 0; }
#console .msg { flex: 1; }

/* Card clicavel (Fase 7): o card inteiro navega para a pagina de detalhe;
   botoes internos (.actions, .card-remove, .pin-btn) usam stopPropagation
   no JS pra nao disparar a navegacao junto. */
.card { cursor: pointer; position: relative; }
.card:hover { border-color: var(--accent); }
.card-remove {
  position: absolute; top: .6rem; right: .6rem; background: none; border: none;
  color: var(--muted); font-size: 1rem; cursor: pointer; line-height: 1; padding: .1rem .3rem;
  border-radius: .25rem;
}
.card-remove:hover { color: var(--err); background: rgba(224,90,90,.12); }
.pin-btn {
  background: none; border: none; color: var(--muted); font-size: .9rem; cursor: pointer;
  padding: 0 .2rem; line-height: 1;
}
.pin-btn.pinned { color: var(--warn); }
.card-head { padding-right: 1.6rem; }

/* Densidade (Fase 7): modo compacto reduz padding/gaps pra caber mais cards. */
body.density-compact #backends-grid { gap: .4rem; grid-template-columns: repeat(auto-fill, minmax(13rem, 1fr)); }
body.density-compact .card { padding: .5rem .6rem; }
body.density-compact .card .counts { margin-top: .3rem; }
body.density-compact .card .actions { margin-top: .4rem; }
body.density-compact .card .meta { display: none; }
"""


def _render_dashboard(
    summary: dict[str, Any],
    servers: list[dict[str, Any]],
    *,
    token: str | None,
    gw_endpoint: str,
) -> str:
    """Gera o shell HTML do dashboard interativo (Fase 7).

    O primeiro paint vem server-rendered (evita tela em branco), e a partir
    daí o JavaScript embutido assume: refresh periódico via ``fetch`` em
    ``/api/servers``/``/health``, ações (disable/enable/restart) via
    ``fetch`` POST, e o console de logs ao vivo via ``EventSource`` em
    ``/api/logs/stream``. Todo valor dinâmico do HTML inicial passa por
    ``html.escape``; o token (se houver) é embutido como uma constante JS
    via ``json.dumps`` (``None`` vira ``null``, string vira literal escapado —
    seguro dentro de ``<script>``), usado só para o próprio navegador
    reautenticar suas chamadas — nunca logado, nunca em outro lugar do HTML.

    ``gw_endpoint`` é a URL do próprio ``POST /mcp`` deste Gateway, calculada
    pelo chamador a partir do host:porta que o navegador já usou pra abrir o
    dashboard (``request.url``) — evita depender de host/porta configurados
    em outro lugar (main.py não repassa isso a ``create_app`` hoje).
    """
    status = str(summary.get("status", "?"))
    status_safe = html.escape(status)
    totals = summary.get("tools_count", 0)
    res_count = summary.get("resources_count", 0)
    prompt_count = summary.get("prompts_count", 0)
    cards_html = _render_backend_cards(servers)
    token_js = json.dumps(token)
    gw_endpoint_safe = html.escape(gw_endpoint)
    return f"""<!DOCTYPE html>  
<html lang="pt-BR">  
<head>  
<meta charset="utf-8">  
<meta name="viewport" content="width=device-width, initial-scale=1">  
<title>MCP Gateway — Dashboard</title>  
<style>{_DASHBOARD_STYLE}</style>
</head>
<body>
<div id="sticky-top">
<header>
  <h1>MCP Gateway</h1>
  <span id="status-pill" class="pill pill-{status_safe}"><span class="dot"></span>{status_safe}</span>
  <span class="stat" id="totals-stat">Tools: {totals} · Resources: {res_count} · Prompts: {prompt_count}</span>
  <span class="grow"></span>
  <span class="gw-endpoint" id="gw-endpoint" title="Endpoint MCP agregado — todo cliente MCP se conecta aqui">{gw_endpoint_safe}</span>
  <span class="grow"></span>
  <label class="switch"><input type="checkbox" id="density-toggle"><span>Compacto</span></label>
  <label class="switch"><input type="checkbox" id="readonly-toggle"><span>Somente leitura</span></label>
  <button id="export-snapshot" class="btn-secondary" title="Baixar JSON com /health + /api/servers + /api/tools/size">Exportar snapshot</button>
  <button id="open-settings" class="btn-secondary">⚙ Configurações</button>
  <button id="open-import-claude" class="btn-secondary">Importar Claude Desktop</button>
  <button id="open-add-mcp" class="btn-primary">+ Adicionar MCP</button>
  <button id="shutdown-gateway" class="btn-danger" title="Encerra o processo do Gateway (graceful shutdown)">Sair do MCP</button>
</header>
<div id="conn-banner" class="conn-banner hidden">⚠ Conexão perdida com o Gateway — tentando reconectar…</div>
</div>
<main>
  <h2>Backends</h2>
  <div id="backends-grid">{cards_html}</div>

  <h2>Console (logs ao vivo)</h2>
  <div id="console-wrap">
    <div id="console-toolbar">
      <span id="console-status">conectando…</span>
      <svg id="error-sparkline" width="60" height="16" class="hidden"></svg>
      <span id="error-rate" class="error-rate hidden">0 erros/min</span>
      <span class="grow"></span>
      <select id="console-level">
        <option value="all">Todos os níveis</option>
        <option value="debug">Debug</option>
        <option value="info">Info</option>
        <option value="warning">Warning</option>
        <option value="error">Error</option>
      </select>
      <input id="console-search" type="text" placeholder="Buscar… ( / )">
      <button id="console-clear-filter" class="btn-secondary hidden">Limpar filtro request_id</button>
      <button id="console-pause" class="btn-secondary">Pausar</button>
      <button id="console-clear" class="btn-secondary">Limpar</button>
    </div>
    <div id="console"><div class="empty">Aguardando eventos…</div></div>
  </div>
</main>
<div id="toast-container"></div>  
  
  
<div id="add-mcp-overlay" class="overlay hidden">  
  <div class="modal">  
    <div class="modal-head">  
      <h3>Adicionar MCP</h3>  
      <button id="close-add-mcp" class="btn-icon">&times;</button>  
    </div>  
    <form id="add-mcp-form">  
      <label>Nome  
        <input type="text" name="name" required placeholder="ex: meu-backend" pattern="[A-Za-z0-9_-]+">  
      </label>  
      <label>Tipo  
        <select name="type" id="add-mcp-type">  
          <option value="stdio">stdio (comando local)</option>  
          <option value="http">http</option>  
          <option value="sse">sse</option>  
        </select>  
      </label>  
      <div id="fields-stdio" class="type-fields">  
        <label>Comando  
          <input type="text" name="command" placeholder="ex: python">  
        </label>  
        <label>Argumentos (um por linha)  
          <textarea name="args" rows="3" placeholder="tests/fake_backend.py"></textarea>  
        </label>  
      </div>  
      <div id="fields-net" class="type-fields hidden">  
        <label>URL  
          <input type="text" name="url" placeholder="http://127.0.0.1:9000">  
        </label>  
      </div>  
      <p id="add-mcp-hint" class="hint">Salva no config.json e já tenta subir o backend na hora.</p>  
      <p id="add-mcp-error" class="error hidden"></p>  
      <div class="modal-actions">  
        <button type="button" id="cancel-add-mcp">Cancelar</button>  
        <button type="submit" id="submit-add-mcp" class="btn-primary">Adicionar</button>  
      </div>  
    </form>  
  </div>
</div>

<div id="settings-overlay" class="overlay hidden">
  <div class="modal">
    <div class="modal-head">
      <h3>Configurações</h3>
      <button id="close-settings" class="btn-icon">&times;</button>
    </div>
    <form id="settings-form">
      <label>Auth token (vazio = sem autenticação)
        <input type="text" name="auth_token" placeholder="deixe vazio para desligar a auth">
      </label>
      <label>Payload máximo do POST /mcp (bytes)
        <input type="number" name="max_payload_bytes" min="1">
      </label>
      <label>Intervalo do Health Monitor (segundos)
        <input type="number" name="health_check_interval_seconds" min="0.1" step="0.1">
      </label>
      <label>Timeout de request a um backend (segundos, vazio = usar o default de 30s)
        <input type="number" name="backend_request_timeout_seconds" min="0.1" step="0.1" placeholder="30">
      </label>
      <label>Tentativas de restart antes de "failed"
        <input type="number" name="max_restart_attempts" min="1" step="1">
      </label>
      <label>TTL de sessão do filtro seletivo (segundos)
        <input type="number" name="session_ttl_seconds" min="1" step="1">
      </label>
      <label class="checkbox-label">
        <input type="checkbox" name="auto_restart"> Reiniciar backends offline automaticamente
      </label>
      <p id="settings-hint" class="hint">Grava no config.json — reinicie o Gateway para aplicar.</p>
      <p id="settings-error" class="error hidden"></p>
      <div class="modal-actions">
        <button type="button" id="cancel-settings">Cancelar</button>
        <button type="submit" id="submit-settings" class="btn-primary">Salvar</button>
      </div>
    </form>
    <div class="settings-divider"></div>
    <div id="backup-restore">
      <h4>Backup do config.json</h4>
      <div class="modal-actions" style="justify-content: flex-start;">
        <button type="button" id="download-backup" class="btn-secondary">Baixar backup</button>
        <button type="button" id="restore-trigger" class="btn-secondary">Restaurar de um backup</button>
        <input type="file" id="restore-file" accept="application/json" style="display:none;">
      </div>
      <p id="restore-error" class="error hidden"></p>
      <p id="restore-hint" class="hint">Restaurar substitui o config.json inteiro (backends inclusos) — valida antes de gravar. Reinicie o Gateway depois.</p>
    </div>
  </div>
</div>

<div id="import-claude-overlay" class="overlay hidden">
  <div class="modal">
    <div class="modal-head">
      <h3>Importar Claude Desktop</h3>
      <button id="close-import-claude" class="btn-icon">&times;</button>
    </div>
    <p id="import-claude-detect-status" class="hint">Procurando o claude_desktop_config.json…</p>
    <form id="import-claude-form">
      <label>Caminho do claude_desktop_config.json
        <input type="text" name="path" id="import-claude-path" placeholder="será preenchido automaticamente se encontrado">
      </label>
      <label>Prefixo dos backends importados (opcional)
        <input type="text" name="prefix" placeholder="ex: claude">
      </label>
      <p id="import-claude-error" class="error hidden"></p>
      <div class="modal-actions">
        <button type="button" id="cancel-import-claude" class="btn-secondary">Cancelar</button>
        <button type="submit" id="submit-import-claude" class="btn-primary">Importar</button>
      </div>
    </form>
  </div>
</div>

<script>  
const GW_TOKEN = {token_js};  
const AUTH_HEADERS = GW_TOKEN ? {{"Authorization": "Bearer " + GW_TOKEN}} : {{}};  
  
// Escapa texto vindo da API antes de entrar em HTML montado por string.
// O schema NÃO restringe command/url/args (só o name é ASCII), então um
// config.json importado pode carregar metacaracteres de HTML nesses campos —
// sem escape, o refresh dos cards despejaria tudo cru no innerHTML
// (DOM XSS acionável por quem influencia o config: arquivo editado à mão,
// importação do Claude Desktop, restore de backup).
function escapeHtml(v) {{  
  return String(v).replace(/[&<>"']/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}}[c]));  
}}  
  
function fmtMeta(s) {{  
  const bits = [];  
  if (s.type) bits.push(s.type);  
  if (s.url) bits.push(s.url);  
  if (s.command) bits.push((s.command + " " + (s.args || []).join(" ")).trim());  
  return escapeHtml(bits.join(" · "));  
}}  
  
// last_restart_at do servidor é um timestamp MONOTÔNICO (não é hora real) —  
// não dá pra converter direto em data. Em vez disso, guardamos QUANDO (hora  
// real local) vimos esse valor mudar pela primeira vez, e humanizamos a  
// partir daí. Funciona enquanto o dashboard ficou aberto desde o restart;  
// se a página abriu depois, mostra "recente" na primeira vez que aparece.  
// Chaves obsoletas são podadas a cada refresh (pruneRestartSeenAt).  
const restartSeenAt = {{}};  
function humanizeRestart(name, lastRestartAt) {{  
  if (lastRestartAt === null || lastRestartAt === undefined) return null;  
  const key = name + ":" + lastRestartAt;  
  if (!(key in restartSeenAt)) restartSeenAt[key] = Date.now();  
  const elapsedMs = Date.now() - restartSeenAt[key];  
  const s = Math.floor(elapsedMs / 1000);  
  if (s < 5) return "agora mesmo";  
  if (s < 60) return `há ${{s}}s`;  
  const m = Math.floor(s / 60);  
  if (m < 60) return `há ${{m}}min`;  
  const h = Math.floor(m / 60);  
  return `há ${{h}}h`;  
}}  
  
// Poda do restartSeenAt: só a entrada do timestamp ATUAL de cada backend
// interessa. Quando um backend troca de timestamp (restart novo) ou some da
// resposta (removido), as chaves antigas viram lixo inacessível — sem esta
// poda, o dicionário cresceria sem limite numa aba aberta por muito tempo
// com muitos restarts.
function pruneRestartSeenAt(servers) {{  
  const live = new Set();  
  for (const s of servers) {{  
    if (s.last_restart_at !== null && s.last_restart_at !== undefined) live.add(s.name + ":" + s.last_restart_at);  
  }}  
  for (const key of Object.keys(restartSeenAt)) {{  
    if (!live.has(key)) delete restartSeenAt[key];  
  }}  
}}  
  
// ---- Favoritos/fixados (localStorage) ----
function loadPinned() {{
  try {{ return new Set(JSON.parse(localStorage.getItem("mcpgw_pinned") || "[]")); }}
  catch (e) {{ return new Set(); }}
}}
function savePinned(set) {{
  localStorage.setItem("mcpgw_pinned", JSON.stringify([...set]));
}}
let pinnedSet = loadPinned();

function renderCard(s, opts) {{
  opts = opts || {{}};
  const st = (s.status || "?").toLowerCase();
  const disabled = st === "disabled";
  const canDisable = st !== "disabled";
  const canEnable = st === "disabled";
  const restartLabel = humanizeRestart(s.name, s.last_restart_at);
  const pinned = pinnedSet.has(s.name);
  return `
    <div class="card" data-name="${{escapeHtml(s.name)}}">
      <button class="card-remove" data-remove="${{escapeHtml(s.name)}}" title="Remover este MCP">&times;</button>
      <div class="card-head">
        <button class="pin-btn${{pinned ? " pinned" : ""}}" data-pin="${{escapeHtml(s.name)}}" title="${{pinned ? "Desafixar" : "Fixar no topo"}}">${{pinned ? "★" : "☆"}}</button>
        <span class="name">${{escapeHtml(s.name)}}</span>
        <span class="badge badge-${{escapeHtml(st)}}">${{escapeHtml(st)}}</span>
      </div>
      <div class="meta">${{fmtMeta(s)}}</div>
      <div class="counts">
        <span>Tools: <b>${{s.tools_count ?? 0}}</b></span>
        <span>Res: <b>${{s.resources_count ?? 0}}</b></span>
        <span>Prompts: <b>${{s.prompts_count ?? 0}}</b></span>
        <span>Falhas: <b>${{s.consecutive_failures ?? 0}}</b></span>
      </div>
      ${{restartLabel ? `<div class="meta">Último restart: ${{escapeHtml(restartLabel)}}</div>` : ""}}
      <div class="actions">
        <button class="act" data-action="restart" ${{disabled ? "disabled" : ""}}>Restart</button>
        <button class="act" data-action="disable" ${{canDisable ? "" : "disabled"}}>Disable</button>
        <button class="act" data-action="enable" ${{canEnable ? "" : "disabled"}}>Enable</button>
      </div>
    </div>`;
}}

const GROUPS = [
  {{ label: "Rodando", statuses: ["running"] }},
  {{ label: "Reiniciando / Offline", statuses: ["restarting", "offline"] }},
  {{ label: "Falhou", statuses: ["failed"] }},
  {{ label: "Desabilitado", statuses: ["disabled"] }},
];

let lastServers = [];

function renderGrid(servers) {{
  lastServers = servers;
  pruneRestartSeenAt(servers);
  const grid = document.getElementById("backends-grid");
  if (!servers.length) {{ grid.innerHTML = '<div class="empty">Nenhum backend configurado.</div>'; return; }}
  let html = "";
  const pinned = servers.filter(s => pinnedSet.has(s.name)).sort((a, b) => a.name.localeCompare(b.name));
  if (pinned.length) {{
    html += `<div class="group-header">📌 Fixados (${{pinned.length}})</div>`;
    html += pinned.map(renderCard).join("");
  }}
  for (const group of GROUPS) {{
    const items = servers
      .filter(s => !pinnedSet.has(s.name) && group.statuses.includes((s.status || "").toLowerCase()))
      .sort((a, b) => a.name.localeCompare(b.name));
    if (!items.length) continue;
    html += `<div class="group-header">${{group.label}} (${{items.length}})</div>`;
    html += items.map(renderCard).join("");
  }}
  grid.innerHTML = html;
}}  
  
// ---- Toasts de mudança de estado ----  
const toastContainer = document.getElementById("toast-container");  
function showToast(msg, kind) {{  
  const el = document.createElement("div");  
  el.className = "toast" + (kind ? " toast-" + kind : "");  
  el.textContent = msg;  
  toastContainer.appendChild(el);  
  requestAnimationFrame(() => el.classList.add("show"));  
  setTimeout(() => {{  
    el.classList.remove("show");  
    setTimeout(() => el.remove(), 250);  
  }}, 4500);  
}}  
let previousStatuses = null;  // null = ainda não teve o primeiro fetch bem-sucedido  
function diffAndToast(servers) {{  
  const current = {{}};  
  for (const s of servers) current[s.name] = (s.status || "").toLowerCase();  
  if (previousStatuses !== null) {{  
    for (const [name, status] of Object.entries(current)) {{  
      const before = previousStatuses[name];  
      if (before !== undefined && before !== status) {{  
        const kind = status === "running" ? "ok" : (status === "failed" ? "err" : "warn");  
        showToast(`${{name}}: ${{before}} → ${{status}}`, kind);  
      }}  
    }}  
  }}  
  previousStatuses = current;  
}}  
  
// ---- Banner de conexão perdida ----  
const connBanner = document.getElementById("conn-banner");  
let fetchFailStreak = 0;  
let sseDown = false;  
function updateConnBanner() {{  
  connBanner.classList.toggle("hidden", !(fetchFailStreak >= 2 || sseDown));  
}}  
  
async function refresh() {{  
  try {{  
    const [healthRes, serversRes] = await Promise.all([  
      fetch("/health"),  
      fetch("/api/servers", {{ headers: AUTH_HEADERS }}),  
    ]);  
    if (healthRes.ok) {{  
      const h = await healthRes.json();  
      const pill = document.getElementById("status-pill");  
      pill.className = "pill pill-" + h.status;  
      pill.innerHTML = '<span class="dot"></span>';  
      pill.appendChild(document.createTextNode(h.status));  
      document.getElementById("totals-stat").textContent =  
        `Tools: ${{h.tools_count}} · Resources: ${{h.resources_count}} · Prompts: ${{h.prompts_count}}`;  
    }}  
    if (serversRes.ok) {{
      const data = await serversRes.json();
      diffAndToast(data.servers || []);
      renderGrid(data.servers || []);
      updateTabTitle(data.servers || []);
    }}  
    fetchFailStreak = 0;  
  }} catch (e) {{  
    fetchFailStreak++;  
  }} finally {{  
    updateConnBanner();  
  }}  
}}  
  
function backendDetailUrl(name) {{
  return `/backend/${{encodeURIComponent(name)}}` + (GW_TOKEN ? "?token=" + encodeURIComponent(GW_TOKEN) : "");
}}

document.getElementById("backends-grid").addEventListener("click", async (ev) => {{
  const card = ev.target.closest(".card");
  if (!card) return;
  const name = card.dataset.name;

  const pinBtn = ev.target.closest(".pin-btn");
  if (pinBtn) {{
    ev.stopPropagation();
    if (pinnedSet.has(name)) pinnedSet.delete(name); else pinnedSet.add(name);
    savePinned(pinnedSet);
    renderGrid(lastServers);
    return;
  }}

  const removeBtn = ev.target.closest(".card-remove");
  if (removeBtn) {{
    ev.stopPropagation();
    const current = lastServers.find(s => s.name === name);
    const st = current ? (current.status || "").toLowerCase() : "";
    const confirmMsg = st === "running"
      ? `⚠ "${{name}}" está RODANDO agora. Remover vai derrubar um backend ativo imediatamente e tirá-lo do config.json — precisa adicionar de novo pra voltar. Continuar?`
      : `Remover o MCP "${{name}}"? Isso para o backend (status atual: ${{st || "desconhecido"}}) e tira ele do config.json — precisa adicionar de novo pra voltar.`;
    if (!confirm(confirmMsg)) return;
    removeBtn.disabled = true;
    try {{
      const res = await fetch(`/api/servers/${{encodeURIComponent(name)}}`, {{
        method: "DELETE", headers: AUTH_HEADERS,
      }});
      if (!res.ok) {{
        const body = await res.json().catch(() => ({{}}));
        alert(`Falha ao remover (${{res.status}}): ${{body.detail || res.statusText}}`);
      }} else {{
        pinnedSet.delete(name);
        savePinned(pinnedSet);
      }}
    }} catch (e) {{
      alert("Erro de rede ao remover.");
    }} finally {{
      await refresh();
    }}
    return;
  }}

  const actBtn = ev.target.closest("button.act");
  if (actBtn) {{
    const action = actBtn.dataset.action;
    card.querySelectorAll("button.act").forEach(b => b.disabled = true);
    try {{
      const res = await fetch(`/api/servers/${{encodeURIComponent(name)}}/${{action}}`, {{
        method: "POST", headers: AUTH_HEADERS,
      }});
      if (!res.ok) {{
        const body = await res.json().catch(() => ({{}}));
        alert(`Falha em ${{action}} (${{res.status}}): ${{body.detail || res.statusText}}`);
      }}
    }} catch (e) {{
      alert("Erro de rede ao executar " + action);
    }} finally {{
      await refresh();
    }}
    return;
  }}

  // Clique no card fora de botões: vai pra página de detalhe do backend.
  window.location.href = backendDetailUrl(name);
}});

// ---- Densidade (compacto/confortável), persiste entre reloads ----
const densityToggle = document.getElementById("density-toggle");
densityToggle.checked = localStorage.getItem("mcpgw_density") === "compact";
document.body.classList.toggle("density-compact", densityToggle.checked);
densityToggle.addEventListener("change", () => {{
  document.body.classList.toggle("density-compact", densityToggle.checked);
  localStorage.setItem("mcpgw_density", densityToggle.checked ? "compact" : "comfortable");
}});

// ---- Sair do MCP: encerra o processo do Gateway (graceful shutdown) ----
document.getElementById("shutdown-gateway").addEventListener("click", async () => {{
  if (!confirm("Encerrar o Gateway agora? Todos os backends serão parados e esta página vai parar de responder.")) return;
  const restarting = lastServers.filter(s => (s.status || "").toLowerCase() === "restarting").map(s => s.name);
  if (restarting.length) {{
    const list = restarting.join(", ");
    if (!confirm(`⚠ ${{restarting.length === 1 ? "O backend" : "Os backends"}} "${{list}}" ${{restarting.length === 1 ? "está" : "estão"}} no meio de uma tentativa de restart agora. Encerrar mesmo assim vai interromper isso na marra. Continuar?`)) return;
  }}
  try {{
    await fetch("/api/shutdown", {{ method: "POST", headers: AUTH_HEADERS }});
  }} catch (e) {{ /* a conexão pode cair antes da resposta chegar — esperado */ }}
  document.title = "⏻ Gateway encerrado";
  document.body.innerHTML = '<div style="padding:3rem;text-align:center;color:#8b91a5;font-family:system-ui">Gateway encerrado. Pode fechar esta aba.</div>';
}});

// ---- Título da aba dinâmico: sinaliza degradação sem precisar olhar a aba ----
function updateTabTitle(servers) {{
  const bad = servers.filter(s => ["offline", "failed"].includes((s.status || "").toLowerCase())).length;
  document.title = bad > 0 ? `⚠ ${{bad}} com problema — MCP Gateway` : "MCP Gateway — Dashboard";
}}

  
refresh();  
setInterval(refresh, 3000);  
  
// ---- Somente leitura (persiste entre reloads) ----  
const readonlyToggle = document.getElementById("readonly-toggle");  
readonlyToggle.checked = localStorage.getItem("mcpgw_readonly") === "1";  
document.body.classList.toggle("readonly-mode", readonlyToggle.checked);  
readonlyToggle.addEventListener("change", () => {{  
  document.body.classList.toggle("readonly-mode", readonlyToggle.checked);  
  localStorage.setItem("mcpgw_readonly", readonlyToggle.checked ? "1" : "0");  
}});  
  
// ---- Exportar snapshot ----  
document.getElementById("export-snapshot").addEventListener("click", async () => {{  
  const btn = document.getElementById("export-snapshot");  
  btn.disabled = true;  
  try {{  
    const [health, servers, toolsSize] = await Promise.all([  
      fetch("/health").then(r => r.json()).catch(() => null),  
      fetch("/api/servers", {{ headers: AUTH_HEADERS }}).then(r => r.json()).catch(() => null),  
      fetch("/api/tools/size", {{ headers: AUTH_HEADERS }}).then(r => r.json()).catch(() => null),  
    ]);  
    const snapshot = {{ captured_at: new Date().toISOString(), health, servers, tools_size: toolsSize }};  
    const blob = new Blob([JSON.stringify(snapshot, null, 2)], {{ type: "application/json" }});  
    const url = URL.createObjectURL(blob);  
    const a = document.createElement("a");  
    a.href = url;  
    a.download = `mcp-gateway-snapshot-${{Date.now()}}.json`;  
    a.click();  
    URL.revokeObjectURL(url);  
  }} finally {{  
    btn.disabled = false;  
  }}  
}});  
  
// ---- Atalhos de teclado ----  
document.addEventListener("keydown", (ev) => {{  
  const tag = document.activeElement ? document.activeElement.tagName : "";  
  if (["INPUT", "TEXTAREA", "SELECT"].includes(tag)) return;  
  if (ev.key === "r") {{ refresh(); }}  
  else if (ev.key === "/") {{ ev.preventDefault(); document.getElementById("console-search").focus(); }}  
}});  

// ---- Modal "Configurações" ----
const settingsOverlay = document.getElementById("settings-overlay");
const settingsForm = document.getElementById("settings-form");
const settingsErrorEl = document.getElementById("settings-error");
const settingsSubmitBtn = document.getElementById("submit-settings");

async function openSettingsModal() {{
  settingsErrorEl.classList.add("hidden");
  settingsOverlay.classList.remove("hidden");
  try {{
    const res = await fetch("/api/config/settings", {{ headers: AUTH_HEADERS }});
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    for (const [key, value] of Object.entries(data)) {{
      const field = settingsForm.elements[key];
      if (!field) continue;
      if (field.type === "checkbox") field.checked = !!value;
      else field.value = (value === null || value === undefined) ? "" : value;
    }}
  }} catch (e) {{
    settingsErrorEl.textContent = "Não consegui carregar as configurações atuais.";
    settingsErrorEl.classList.remove("hidden");
  }}
}}
function closeSettingsModal() {{ settingsOverlay.classList.add("hidden"); }}

document.getElementById("open-settings").addEventListener("click", openSettingsModal);
document.getElementById("close-settings").addEventListener("click", closeSettingsModal);
document.getElementById("cancel-settings").addEventListener("click", closeSettingsModal);
settingsOverlay.addEventListener("click", (ev) => {{ if (ev.target === settingsOverlay) closeSettingsModal(); }});

settingsForm.addEventListener("submit", async (ev) => {{
  ev.preventDefault();
  settingsErrorEl.classList.add("hidden");
  const data = new FormData(settingsForm);
  const payload = {{
    auth_token: data.get("auth_token") || null,
    max_payload_bytes: Number(data.get("max_payload_bytes")),
    health_check_interval_seconds: Number(data.get("health_check_interval_seconds")),
    max_restart_attempts: Number(data.get("max_restart_attempts")),
    session_ttl_seconds: Number(data.get("session_ttl_seconds")),
    auto_restart: settingsForm.elements["auto_restart"].checked,
  }};
  const timeoutRaw = data.get("backend_request_timeout_seconds");
  payload.backend_request_timeout_seconds = timeoutRaw ? Number(timeoutRaw) : null;

  settingsSubmitBtn.disabled = true;
  settingsSubmitBtn.textContent = "Salvando…";
  try {{
    const res = await fetch("/api/config/settings", {{
      method: "PUT",
      headers: {{ "Content-Type": "application/json", ...AUTH_HEADERS }},
      body: JSON.stringify(payload),
    }});
    const body = await res.json().catch(() => ({{}}));
    if (!res.ok) {{
      settingsErrorEl.textContent = body.detail || `Erro ${{res.status}}`;
      settingsErrorEl.classList.remove("hidden");
      return;
    }}
    closeSettingsModal();
    alert(body.detail || "Configurações salvas. Reinicie o Gateway para aplicar.");
  }} catch (e) {{
    settingsErrorEl.textContent = "Erro de rede ao salvar.";
    settingsErrorEl.classList.remove("hidden");
  }} finally {{
    settingsSubmitBtn.disabled = false;
    settingsSubmitBtn.textContent = "Salvar";
  }}
}});

// ---- Backup / restore do config.json ----
document.getElementById("download-backup").addEventListener("click", async () => {{
  const btn = document.getElementById("download-backup");
  btn.disabled = true;
  try {{
    const res = await fetch("/api/config/backup", {{ headers: AUTH_HEADERS }});
    if (!res.ok) throw new Error("HTTP " + res.status);
    const text = await res.text();
    const blob = new Blob([text], {{ type: "application/json" }});
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `mcp-gateway-config-backup-${{Date.now()}}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }} catch (e) {{
    alert("Erro ao baixar o backup.");
  }} finally {{
    btn.disabled = false;
  }}
}});

document.getElementById("restore-trigger").addEventListener("click", () => {{
  document.getElementById("restore-file").click();
}});

document.getElementById("restore-file").addEventListener("change", async (ev) => {{
  const restoreErrorEl = document.getElementById("restore-error");
  restoreErrorEl.classList.add("hidden");
  const file = ev.target.files[0];
  if (!file) return;
  if (!confirm(`Restaurar "${{file.name}}"? Isso SUBSTITUI o config.json inteiro (todos os backends). Precisa reiniciar o Gateway depois. Continuar?`)) {{
    ev.target.value = "";
    return;
  }}
  try {{
    const text = await file.text();
    const res = await fetch("/api/config/restore", {{
      method: "POST",
      headers: {{ "Content-Type": "application/json", ...AUTH_HEADERS }},
      body: text,
    }});
    const body = await res.json().catch(() => ({{}}));
    if (!res.ok) {{
      restoreErrorEl.textContent = body.detail || `Erro ${{res.status}}`;
      restoreErrorEl.classList.remove("hidden");
      return;
    }}
    alert(body.detail || "Config restaurado. Reinicie o Gateway para aplicar.");
  }} catch (e) {{
    restoreErrorEl.textContent = "Erro de rede ou arquivo inválido.";
    restoreErrorEl.classList.remove("hidden");
  }} finally {{
    ev.target.value = "";
  }}
}});

// ---- Modal "Importar Claude Desktop" ----
const importClaudeOverlay = document.getElementById("import-claude-overlay");
const importClaudeForm = document.getElementById("import-claude-form");
const importClaudeErrorEl = document.getElementById("import-claude-error");
const importClaudeSubmitBtn = document.getElementById("submit-import-claude");
const importClaudeDetectStatus = document.getElementById("import-claude-detect-status");

async function openImportClaudeModal() {{
  importClaudeErrorEl.classList.add("hidden");
  importClaudeForm.reset();
  importClaudeOverlay.classList.remove("hidden");
  importClaudeDetectStatus.textContent = "Procurando o claude_desktop_config.json…";
  try {{
    const res = await fetch("/api/import/claude-desktop/detect", {{ headers: AUTH_HEADERS }});
    const data = await res.json();
    if (data.found) {{
      document.getElementById("import-claude-path").value = data.path;
      importClaudeDetectStatus.textContent = `Encontrado em: ${{data.path}}`;
    }} else {{
      importClaudeDetectStatus.textContent = "Não encontrei automaticamente — informe o caminho abaixo.";
    }}
  }} catch (e) {{
    importClaudeDetectStatus.textContent = "Não consegui buscar automaticamente — informe o caminho abaixo.";
  }}
}}
function closeImportClaudeModal() {{ importClaudeOverlay.classList.add("hidden"); }}

document.getElementById("open-import-claude").addEventListener("click", openImportClaudeModal);
document.getElementById("close-import-claude").addEventListener("click", closeImportClaudeModal);
document.getElementById("cancel-import-claude").addEventListener("click", closeImportClaudeModal);
importClaudeOverlay.addEventListener("click", (ev) => {{ if (ev.target === importClaudeOverlay) closeImportClaudeModal(); }});

importClaudeForm.addEventListener("submit", async (ev) => {{
  ev.preventDefault();
  importClaudeErrorEl.classList.add("hidden");
  const data = new FormData(importClaudeForm);
  const payload = {{ path: data.get("path") || null, prefix: data.get("prefix") || "" }};
  importClaudeSubmitBtn.disabled = true;
  importClaudeSubmitBtn.textContent = "Importando…";
  try {{
    const res = await fetch("/api/import/claude-desktop", {{
      method: "POST",
      headers: {{ "Content-Type": "application/json", ...AUTH_HEADERS }},
      body: JSON.stringify(payload),
    }});
    const body = await res.json().catch(() => ({{}}));
    if (!res.ok) {{
      importClaudeErrorEl.textContent = body.detail || `Erro ${{res.status}}`;
      importClaudeErrorEl.classList.remove("hidden");
      return;
    }}
    closeImportClaudeModal();
    let msg = body.detail || "Importado.";
    if (body.warnings && body.warnings.length) {{
      msg += "\\n\\nAvisos:\\n" + body.warnings.join("\\n");
    }}
    alert(msg);
  }} catch (e) {{
    importClaudeErrorEl.textContent = "Erro de rede ao importar.";
    importClaudeErrorEl.classList.remove("hidden");
  }} finally {{
    importClaudeSubmitBtn.disabled = false;
    importClaudeSubmitBtn.textContent = "Importar";
  }}
}});

  
  
// ---- Console de logs ao vivo (SSE) ----  
const consoleEl = document.getElementById("console");  
const statusEl = document.getElementById("console-status");  
const pauseBtn = document.getElementById("console-pause");  
const clearBtn = document.getElementById("console-clear");  
const levelSelect = document.getElementById("console-level");  
const searchInput = document.getElementById("console-search");  
const clearFilterBtn = document.getElementById("console-clear-filter");  
const errorRateEl = document.getElementById("error-rate");  
const MAX_LINES = 500;  
let paused = false;  
let allEvents = [];       // buffer local dos últimos MAX_LINES eventos (para poder filtrar retroativamente)  
let requestIdFilter = null;  
  
function fmtTs(ts) {{  
  if (ts === undefined || ts === null || ts === "") return "";  
  // structlog manda ISO string (TimeStamper fmt="iso"); mas o replay/eventos  
  // sintéticos podem vir como epoch em segundos — aceita os dois formatos.  
  const d = typeof ts === "number" ? new Date(ts * 1000) : new Date(ts);  
  return isNaN(d.getTime()) ? "" : d.toLocaleTimeString();  
}}  
  
function hashColor(str) {{  
  let hash = 0;  
  for (let i = 0; i < str.length; i++) hash = (hash * 31 + str.charCodeAt(i)) >>> 0;  
  return `hsl(${{hash % 360}}, 65%, 55%)`;  
}}  
  
function passesFilter(evt) {{  
  const lvl = (evt.level || "info").toLowerCase();  
  if (levelSelect.value !== "all" && lvl !== levelSelect.value) return false;  
  if (requestIdFilter && evt.request_id !== requestIdFilter) return false;  
  const q = searchInput.value.trim().toLowerCase();  
  if (q && !JSON.stringify(evt).toLowerCase().includes(q)) return false;  
  return true;  
}}  
  
function buildLineEl(evt) {{  
  const lvl = (evt.level || "info").toLowerCase();  
  const div = document.createElement("div");  
  div.className = "line lvl-" + lvl + (evt.request_id && evt.request_id === requestIdFilter ? " req-active" : "");  
  if (evt.request_id) {{  
    const dot = document.createElement("span");  
    dot.className = "reqdot";  
    dot.style.background = hashColor(evt.request_id);  
    dot.title = "request_id: " + evt.request_id + " (clique pra filtrar)";  
    div.appendChild(dot);  
  }}  
  const ts = document.createElement("span");  
  ts.className = "ts";  
  ts.textContent = fmtTs(evt.timestamp);  
  div.appendChild(ts);  
  const msg = document.createElement("span");  
  msg.className = "msg";  
  msg.textContent = evt.event || JSON.stringify(evt);  
  div.appendChild(msg);  
  if (evt.request_id) {{  
    div.addEventListener("click", () => {{  
      requestIdFilter = requestIdFilter === evt.request_id ? null : evt.request_id;  
      clearFilterBtn.classList.toggle("hidden", !requestIdFilter);  
      renderConsole();  
    }});  
  }}  
  return div;  
}}  
  
function renderConsole() {{  
  const filtered = allEvents.filter(passesFilter);  
  consoleEl.innerHTML = "";  
  if (!filtered.length) {{  
    consoleEl.innerHTML = '<div class="empty">Nenhum evento (ainda, ou filtrado).</div>';  
  }} else {{  
    for (const evt of filtered) consoleEl.appendChild(buildLineEl(evt));  
    consoleEl.scrollTop = consoleEl.scrollHeight;  
  }}  
}}  
  
const sparklineEl = document.getElementById("error-sparkline");
function errorTimestampMs(evt) {{
  const t = typeof evt.timestamp === "number" ? evt.timestamp * 1000 : new Date(evt.timestamp).getTime();
  return isNaN(t) ? null : t;
}}
function updateErrorRate() {{
  const now = Date.now();
  const errorTimes = allEvents
    .filter(evt => {{ const lvl = (evt.level || "").toLowerCase(); return lvl === "error" || lvl === "critical"; }})
    .map(errorTimestampMs)
    .filter(t => t !== null);
  const recentErrors = errorTimes.filter(t => (now - t) <= 60000).length;
  errorRateEl.textContent = `${{recentErrors}} erro${{recentErrors === 1 ? "" : "s"}}/min`;
  errorRateEl.classList.toggle("hidden", recentErrors === 0 && errorTimes.length === 0);

  // Sparkline: 15 baldes de 1 minuto cada, últimos 15 minutos.
  const BUCKETS = 15, BUCKET_MS = 60000;
  const counts = new Array(BUCKETS).fill(0);
  for (const t of errorTimes) {{
    const age = now - t;
    if (age < 0 || age >= BUCKETS * BUCKET_MS) continue;
    counts[BUCKETS - 1 - Math.floor(age / BUCKET_MS)]++;
  }}
  const max = Math.max(1, ...counts);
  const barW = 60 / BUCKETS;
  const bars = counts.map((c, i) => {{
    const h = Math.max(1, (c / max) * 14);
    return `<rect x="${{(i * barW).toFixed(1)}}" y="${{(16 - h).toFixed(1)}}" width="${{(barW - 1).toFixed(1)}}" height="${{h.toFixed(1)}}" fill="var(--err)" opacity="${{c ? 0.9 : 0.15}}" />`;
  }}).join("");
  sparklineEl.innerHTML = bars;
  sparklineEl.classList.toggle("hidden", errorTimes.length === 0);
}}  
setInterval(updateErrorRate, 5000);  
  
function ingestEvent(evt) {{  
  if (paused) return;  
  allEvents.push(evt);  
  if (allEvents.length > MAX_LINES) allEvents = allEvents.slice(-MAX_LINES);  
  renderConsole();  
  updateErrorRate();  
}}  
  
pauseBtn.addEventListener("click", () => {{  
  paused = !paused;  
  pauseBtn.textContent = paused ? "Retomar" : "Pausar";  
}});  
clearBtn.addEventListener("click", () => {{ allEvents = []; renderConsole(); updateErrorRate(); }});  
levelSelect.addEventListener("change", renderConsole);  
searchInput.addEventListener("input", renderConsole);  
clearFilterBtn.addEventListener("click", () => {{  
  requestIdFilter = null;  
  clearFilterBtn.classList.add("hidden");  
  renderConsole();  
}});  
  
const streamUrl = "/api/logs/stream" + (GW_TOKEN ? "?token=" + encodeURIComponent(GW_TOKEN) : "");  
const source = new EventSource(streamUrl);  
source.onopen = () => {{ statusEl.textContent = "conectado"; sseDown = false; updateConnBanner(); }};  
source.onerror = () => {{ statusEl.textContent = "reconectando…"; sseDown = true; updateConnBanner(); }};  
source.onmessage = (e) => {{  
  try {{ ingestEvent(JSON.parse(e.data)); }} catch (err) {{ /* linha não-JSON, ignora */ }}  
}};  
  
  
// ---- Modal "Adicionar MCP" ----  
const overlay = document.getElementById("add-mcp-overlay");  
const form = document.getElementById("add-mcp-form");  
const typeSelect = document.getElementById("add-mcp-type");  
const errorEl = document.getElementById("add-mcp-error");  
const submitBtn = document.getElementById("submit-add-mcp");  
  
function openModal() {{  
  errorEl.classList.add("hidden");  
  form.reset();  
  toggleTypeFields();  
  overlay.classList.remove("hidden");  
  form.querySelector('[name="name"]').focus();  
}}  
function closeModal() {{ overlay.classList.add("hidden"); }}  
  
function toggleTypeFields() {{  
  const isStdio = typeSelect.value === "stdio";  
  document.getElementById("fields-stdio").classList.toggle("hidden", !isStdio);  
  document.getElementById("fields-net").classList.toggle("hidden", isStdio);  
}}  
  
document.getElementById("open-add-mcp").addEventListener("click", openModal);  
document.getElementById("close-add-mcp").addEventListener("click", closeModal);  
document.getElementById("cancel-add-mcp").addEventListener("click", closeModal);  
overlay.addEventListener("click", (ev) => {{ if (ev.target === overlay) closeModal(); }});  
typeSelect.addEventListener("change", toggleTypeFields);  
  
form.addEventListener("submit", async (ev) => {{  
  ev.preventDefault();  
  errorEl.classList.add("hidden");  
  const data = new FormData(form);  
  const payload = {{ name: (data.get("name") || "").trim(), type: data.get("type") }};  
  if (payload.type === "stdio") {{  
    payload.command = (data.get("command") || "").trim();  
    payload.args = (data.get("args") || "")  
      .split("\\n").map(s => s.trim()).filter(Boolean);  
  }} else {{  
    payload.url = (data.get("url") || "").trim();  
  }}  
  submitBtn.disabled = true;  
  submitBtn.textContent = "Adicionando…";  
  try {{  
    const res = await fetch("/api/config/backends", {{  
      method: "POST",  
      headers: {{ "Content-Type": "application/json", ...AUTH_HEADERS }},  
      body: JSON.stringify(payload),  
    }});  
    const body = await res.json().catch(() => ({{}}));  
    if (!res.ok) {{  
      errorEl.textContent = body.detail || `Erro ${{res.status}}`;  
      errorEl.classList.remove("hidden");  
      return;  
    }}  
    closeModal();  
    alert(body.detail || "Backend adicionado. Reinicie o Gateway para aplicar.");  
  }} catch (e) {{  
    errorEl.textContent = "Erro de rede ao salvar.";  
    errorEl.classList.remove("hidden");  
  }} finally {{  
    submitBtn.disabled = false;  
    submitBtn.textContent = "Adicionar";  
  }}  
}});  
</script>  
</body>  
</html>"""


def _render_backend_cards(servers: list[dict[str, Any]]) -> str:
    """Renderiza os cards iniciais (server-rendered) dos backends.

    Primeiro paint com a MESMA estrutura visual dos cards do ``renderCard``
    (JS) — ``data-name``, badge, meta, contagens e botões restart/disable/
    enable funcionais via delegador de cliques — para a tela não ficar em
    branco antes do primeiro ``refresh``. É porém uma versão REDUZIDA: sem
    ``card-remove`` nem ``pin-btn``. O de fixar não pode ser emitido pelo
    servidor porque o estado dos fixados vive no localStorage do navegador
    (``pinnedSet`` é client-side por design — emitir um default ☆ fixaria
    errado e um clique desafixaria por engano); os dois botões aparecem no
    primeiro ``refresh`` (até ~3s), quando o ``renderGrid`` substitui estes
    cards pelo render completo do JS. Todo valor dinâmico passa por
    ``html.escape``.
    """
    if not servers:
        return '<div class="empty">Nenhum backend configurado.</div>'
    cards: list[str] = []
    for s in servers:
        status = str(s.get("status", "?")).lower()
        name = html.escape(str(s.get("name", "")))
        meta_bits = [
            str(s.get("type") or ""),
            str(s.get("url") or ""),
            " ".join([str(s.get("command") or ""), *s.get("args", [])]).strip(),
        ]
        meta = html.escape(" · ".join(b for b in meta_bits if b))
        disabled_attr = "disabled" if status == "disabled" else ""
        can_disable = "" if status != "disabled" else "disabled"
        can_enable = "" if status == "disabled" else "disabled"
        cards.append(f"""  
        <div class="card" data-name="{name}">  
          <div class="card-head">  
            <span class="name">{name}</span>  
            <span class="badge badge-{html.escape(status)}">{html.escape(status)}</span>  
          </div>  
          <div class="meta">{meta}</div>  
          <div class="counts">  
            <span>Tools: <b>{s.get("tools_count", 0)}</b></span>  
            <span>Res: <b>{s.get("resources_count", 0)}</b></span>  
            <span>Prompts: <b>{s.get("prompts_count", 0)}</b></span>  
            <span>Falhas: <b>{s.get("consecutive_failures", 0)}</b></span>  
          </div>  
          <div class="actions">  
            <button class="act" data-action="restart" {disabled_attr}>Restart</button>  
            <button class="act" data-action="disable" {can_disable}>Disable</button>  
            <button class="act" data-action="enable" {can_enable}>Enable</button>  
          </div>  
        </div>""")
    return "\n".join(cards)


_DETAIL_STYLE = """
:root {
  color-scheme: light dark;
  --bg: #0f1117; --panel: #171a23; --panel-2: #1d212c; --border: #2a2f3d;
  --text: #e6e8ef; --muted: #8b91a5; --accent: #5b8cff;
  --ok: #34c77b; --warn: #e0a63a; --err: #e05a5a; --off: #6b7284;
}
* { box-sizing: border-box; }
body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; margin: 0; background: var(--bg); color: var(--text); }
header { display: flex; align-items: center; gap: 1rem; flex-wrap: wrap; padding: 1rem 1.5rem; border-bottom: 1px solid var(--border); }
header h1 { font-size: 1.05rem; margin: 0; font-weight: 600; }
a.back { color: var(--muted); text-decoration: none; font-size: .85rem; }
a.back:hover { color: var(--accent); }
.badge { font-size: .72rem; font-weight: 700; text-transform: uppercase; letter-spacing: .03em; padding: .12rem .5rem; border-radius: .3rem; }
.badge-running { background: rgba(52,199,123,.15); color: var(--ok); }
.badge-offline, .badge-failed { background: rgba(224,90,90,.15); color: var(--err); }
.badge-restarting { background: rgba(224,166,58,.15); color: var(--warn); }
.badge-disabled { background: rgba(107,114,132,.2); color: var(--off); }
main { padding: 1.25rem 1.5rem 3rem; max-width: 60rem; margin: 0 auto; }
h2 { font-size: .9rem; text-transform: uppercase; letter-spacing: .04em; color: var(--muted); margin: 1.75rem 0 .6rem; }
.info-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(14rem, 1fr)); gap: .8rem; }
.info-card { background: var(--panel); border: 1px solid var(--border); border-radius: .6rem; padding: .8rem 1rem; }
.info-card .label { font-size: .72rem; color: var(--muted); text-transform: uppercase; letter-spacing: .03em; }
.info-card .value { font-size: .95rem; margin-top: .2rem; word-break: break-all; }
.item-list { display: flex; flex-direction: column; gap: .4rem; }
.item {
  background: var(--panel); border: 1px solid var(--border); border-radius: .5rem;
  padding: .55rem .8rem;
}
.item .item-name { font-weight: 600; font-family: ui-monospace, monospace; font-size: .85rem; }
.item .item-desc { color: var(--muted); font-size: .8rem; margin-top: .15rem; }
.empty { color: var(--muted); font-size: .82rem; padding: .5rem 0; }
.grow { flex: 1; }
.actions { display: flex; gap: .4rem; }
button.act {
  background: var(--panel-2); border: 1px solid var(--border); color: var(--text);
  padding: .35rem .7rem; border-radius: .4rem; font-size: .8rem; cursor: pointer;
}
button.act:hover { border-color: var(--accent); }
button.act:disabled { opacity: .4; cursor: not-allowed; }
"""


def _render_backend_detail(
    state_detail: dict[str, Any], entries: dict[str, list[Any]], *, token: str | None
) -> str:
    """Gera a página de detalhe de UM backend (Fase 7 — clique no card).

    ``state_detail`` é o dict que ``server_details()`` já produz para este
    backend (reaproveitado, não reinventado). ``entries`` traz as três listas
    de ``RegistryEntry`` (tools/resources/prompts) já filtradas por esse
    backend — a ordenação alfabética e a extração de descrição acontecem
    aqui, perto da renderização. ``token`` é reembutido como constante JS
    (mesmo padrão do dashboard) para os botões de ação (restart/disable/
    enable) reautenticarem o ``fetch`` — a página não é só leitura.
    """
    name = html.escape(str(state_detail.get("name", "")))
    name_js = json.dumps(str(state_detail.get("name", "")))
    status = str(state_detail.get("status", "?"))
    status_safe = html.escape(status)
    status_raw = status.lower()
    token_js = json.dumps(token)
    connected_since = state_detail.get("connected_since")
    uptime_html = ""
    if connected_since:
        connected_dt = html.escape(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(connected_since))
        )
        uptime_html = f"""
      <div class="info-card">
        <div class="label">Conectado desde</div>
        <div class="value">{connected_dt}</div>
      </div>
      <div class="info-card">
        <div class="label">Tempo ativo</div>
        <div class="value" id="uptime" data-since="{connected_since}">calculando…</div>
      </div>"""

    meta_bits = [
        ("Tipo", state_detail.get("type")),
        ("URL", state_detail.get("url")),
        ("Comando", state_detail.get("command")),
        ("Argumentos", " ".join(state_detail.get("args") or []) or None),
        ("Transporte", state_detail.get("transport")),
        ("Falhas consecutivas", state_detail.get("consecutive_failures")),
    ]
    info_cards = uptime_html
    for label, value in meta_bits:
        if value in (None, ""):
            continue
        info_cards += f"""
      <div class="info-card">
        <div class="label">{html.escape(label)}</div>
        <div class="value">{html.escape(str(value))}</div>
      </div>"""

    def render_items(kind_entries: list[Any], id_field: str) -> str:
        if not kind_entries:
            return '<div class="empty">Nenhum item.</div>'
        sorted_entries = sorted(kind_entries, key=lambda e: e.name.lower())
        items = []
        for entry in sorted_entries:
            item_name = html.escape(entry.name)
            description = entry.metadata.get("description") or ""
            desc_html = f'<div class="item-desc">{html.escape(str(description))}</div>' if description else ""
            items.append(
                f'<div class="item"><div class="item-name">{item_name}</div>{desc_html}</div>'
            )
        return '<div class="item-list">' + "\n".join(items) + "</div>"

    tools_html = render_items(entries.get("tools", []), "name")
    resources_html = render_items(entries.get("resources", []), "uri")
    prompts_html = render_items(entries.get("prompts", []), "name")
    is_disabled = status_raw == "disabled"

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{name} — MCP Gateway</title>
<style>{_DETAIL_STYLE}</style>
</head>
<body>
<header>
  <a class="back" href="javascript:history.back()">← Voltar</a>
  <h1>{name}</h1>
  <span id="detail-status" class="badge badge-{status_safe}">{status_safe}</span>
  <span class="grow"></span>
  <div class="actions">
    <button class="act" id="act-restart" data-action="restart" {"disabled" if is_disabled else ""}>Restart</button>
    <button class="act" id="act-disable" data-action="disable" {"disabled" if is_disabled else ""}>Disable</button>
    <button class="act" id="act-enable" data-action="enable" {"" if is_disabled else "disabled"}>Enable</button>
  </div>
</header>
<main>
  <h2>Informações</h2>
  <div class="info-grid">{info_cards}</div>

  <h2>Tools ({len(entries.get("tools", []))})</h2>
  {tools_html}

  <h2>Resources ({len(entries.get("resources", []))})</h2>
  {resources_html}

  <h2>Prompts ({len(entries.get("prompts", []))})</h2>
  {prompts_html}

  <h2>Histórico (últimos ciclos do Health Monitor)</h2>
  <div id="history-wrap" class="info-card">
    <div id="history-status" class="hint">Carregando histórico…</div>
    <svg id="history-chart" width="100%" height="90" viewBox="0 0 600 90" preserveAspectRatio="none" style="display:none; margin-top:.5rem;"></svg>
    <div id="history-timeline" style="display:flex; gap:1px; margin-top:.6rem;"></div>
  </div>
</main>
<script>
const GW_TOKEN = {token_js};
const BACKEND_NAME = {name_js};
const AUTH_HEADERS = GW_TOKEN ? {{"Authorization": "Bearer " + GW_TOKEN}} : {{}};

const STATUS_COLOR = {{
  running: "#34c77b", restarting: "#e0a63a", offline: "#e05a5a",
  failed: "#e05a5a", disabled: "#6b7284",
}};

async function loadHistory() {{
  const statusEl = document.getElementById("history-status");
  const chartEl = document.getElementById("history-chart");
  const timelineEl = document.getElementById("history-timeline");
  try {{
    const res = await fetch(`/api/servers/${{encodeURIComponent(BACKEND_NAME)}}/history`, {{ headers: AUTH_HEADERS }});
    const data = await res.json();
    const samples = data.samples || [];
    if (!samples.length) {{
      statusEl.textContent = "Sem amostras ainda — aguarde alguns ciclos do Health Monitor.";
      return;
    }}
    statusEl.textContent = `${{samples.length}} amostra(s) — últimas ${{Math.round((samples[samples.length-1].timestamp - samples[0].timestamp) / 60)}}min`;

    // Timeline de status: um bloco colorido por amostra, mais recente à direita.
    timelineEl.innerHTML = samples.map(s =>
      `<div title="${{s.status}} — ${{new Date(s.timestamp * 1000).toLocaleTimeString()}}" style="flex:1; height:14px; background:${{STATUS_COLOR[s.status] || '#6b7284'}};"></div>`
    ).join("");

    // Sparkline de latência: só pontos onde houve ping de verdade (latency_ms != null).
    const latencyPoints = samples.map((s, i) => ({{i, v: s.latency_ms}})).filter(p => p.v !== null && p.v !== undefined);
    if (latencyPoints.length >= 2) {{
      const maxV = Math.max(1, ...latencyPoints.map(p => p.v));
      const stepX = 600 / Math.max(1, samples.length - 1);
      const pts = latencyPoints.map(p => `${{(p.i * stepX).toFixed(1)}},${{(85 - (p.v / maxV) * 80).toFixed(1)}}`).join(" ");
      chartEl.innerHTML = `<polyline points="${{pts}}" fill="none" stroke="#5b8cff" stroke-width="2" />`;
      chartEl.style.display = "block";
      statusEl.textContent += ` — latência atual: ${{latencyPoints[latencyPoints.length-1].v.toFixed(1)}}ms (máx no período: ${{maxV.toFixed(1)}}ms)`;
    }}
  }} catch (e) {{
    statusEl.textContent = "Não consegui carregar o histórico.";
  }}
}}
loadHistory();
setInterval(loadHistory, 10000);

const uptimeEl = document.getElementById("uptime");
if (uptimeEl) {{
  const since = parseFloat(uptimeEl.dataset.since) * 1000;
  function fmt() {{
    const s = Math.max(0, Math.floor((Date.now() - since) / 1000));
    const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
    uptimeEl.textContent = h > 0 ? `${{h}}h ${{m}}min` : (m > 0 ? `${{m}}min ${{sec}}s` : `${{sec}}s`);
  }}
  fmt();
  setInterval(fmt, 1000);
}}

document.querySelectorAll("button.act").forEach(btn => {{
  btn.addEventListener("click", async () => {{
    const action = btn.dataset.action;
    document.querySelectorAll("button.act").forEach(b => b.disabled = true);
    try {{
      const res = await fetch(`/api/servers/${{encodeURIComponent(BACKEND_NAME)}}/${{action}}`, {{
        method: "POST", headers: AUTH_HEADERS,
      }});
      const body = await res.json().catch(() => ({{}}));
      if (!res.ok) {{
        alert(`Falha em ${{action}} (${{res.status}}): ${{body.detail || res.statusText}}`);
      }} else if (body.status) {{
        const badge = document.getElementById("detail-status");
        badge.className = "badge badge-" + body.status;
        badge.textContent = body.status;
      }}
    }} catch (e) {{
      alert("Erro de rede ao executar " + action);
    }} finally {{
      // Recarrega a página inteira: garante que os botões habilitados/
      // desabilitados e todo o resto da info (falhas, conectado desde) reflitam
      // o estado real pós-ação, sem duplicar a lógica de re-render do dashboard.
      window.location.reload();
    }}
  }});
}});
</script>
</body>
</html>"""


def _validate_new_backend_payload(payload: Any) -> str | None:
    """Valida o corpo de ``POST /api/config/backends``.

    Espelha as regras que ``BackendConfig`` exige: ``stdio`` precisa de
    ``command`` (e ``args`` opcional como lista de strings); ``http``/``sse``
    precisam de ``url`` começando com ``http://`` ou ``https://``. O ``name``
    é obrigatório e validado contra o MESMO padrão ASCII do schema
    (``BACKEND_NAME_PATTERN`` — ver ``_BACKEND_NAME_RE``): letras, números,
    ``-`` e ``_``, sem acentos nem ``.``.

    Returns:
        A mensagem de erro (string) do primeiro problema encontrado, ou
        ``None`` se o payload for válido.
    """
    if not isinstance(payload, dict):
        return "corpo deve ser um objeto JSON"
    name = payload.get("name")
    if not isinstance(name, str) or not name.strip():
        return "campo 'name' é obrigatório"
    if _BACKEND_NAME_RE.match(name.strip()) is None:
        return "'name' só pode ter letras ASCII, números, '-' e '_'"
    btype = payload.get("type", "stdio")
    if btype not in ("stdio", "http", "sse"):
        return "'type' deve ser 'stdio', 'http' ou 'sse'"
    if btype == "stdio":
        command = payload.get("command")
        if not isinstance(command, str) or not command.strip():
            return "'command' é obrigatório para type=stdio"
        args = payload.get("args", [])
        if not isinstance(args, list) or not all(isinstance(a, str) for a in args):
            return "'args' deve ser uma lista de strings"
    else:
        url = payload.get("url")
        if not isinstance(url, str) or not url.strip().lower().startswith(("http://", "https://")):
            return "'url' é obrigatório e deve começar com http:// ou https://"
    return None


def _build_backend_entry(payload: dict[str, Any]) -> dict[str, Any]:
    """Monta a entrada a ser gravada em ``config.json`` a partir do payload
    já validado.

    Shape PADRONIZADO entre os escritores de config: backends ``stdio``
    carregam ``type: "stdio"`` EXPLÍCITO — igual ao que o importador do
    Claude Desktop emite (``convert_entry``) — para o mesmo arquivo não ficar
    heterogêneo conforme o caminho que criou cada entrada. Antes, esta rota
    omitia a chave (espelhando o ``backend-a`` do config original); o schema
    aceita as duas formas (default ``stdio``, ver ``BackendConfig.type``),
    então a mudança é cosmética. ``args`` segue só presente quando há
    argumentos; ``http``/``sse`` carregam ``type`` + ``url``.
    """
    name = payload["name"].strip()
    btype = payload.get("type", "stdio")
    if btype == "stdio":
        entry: dict[str, Any] = {
            "name": name,
            "type": "stdio",
            "command": payload["command"].strip(),
        }
        args = [a for a in (payload.get("args") or []) if a]
        if args:
            entry["args"] = args
        return entry
    return {"name": name, "type": btype, "url": payload["url"].strip()}


def _dashboard_browser_url(auth_token: str | None) -> str:
    """URL que o auto-open do dashboard abre no navegador.

    O host deriva de ``MCP_GATEWAY_HOST`` — o MESMO valor (e a MESMA
    normalização: strip, fallback) que o ``main.py`` usa para o bind — então
    o navegador abre o endereço em que o Gateway realmente escuta (antes, a
    URL era fixa em 127.0.0.1 e ignorava o host configurado). Duas exceções
    deliberadas:

    - ``0.0.0.0`` (bind "todas as interfaces") não é endereço navegável —
      navegadores abrem ``127.0.0.1`` no lugar; mapear para o loopback é
      exatamente o que ``0.0.0.0`` significa para um cliente na própria
      máquina, então o auto-open continua funcionando nesse cenário;
    - hosts IPv6 ganham colchetes (``http://[::1]:porta/``), sem os quais a
      URL ficaria malformada.

    Sem env, ``main.py`` faz bind em 127.0.0.1 e a URL bate por definição —
    o comportamento do uso local não muda.
    """
    host = os.environ.get("MCP_GATEWAY_HOST", "127.0.0.1").strip() or "127.0.0.1"
    if host == "0.0.0.0":
        host = "127.0.0.1"
    elif ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = os.environ.get("MCP_GATEWAY_PORT", "8080")
    url = f"http://{host}:{port}/"
    if auth_token:
        url += f"?token={auth_token}"
    return url


def create_app(  
    mcp_server: McpServer,  
    *,  
    auth_token: str | None = None,  
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,  
) -> FastAPI:  
    """Monta a aplicação FastAPI do Gateway: rota MCP, dashboard e API HTTP.

    Reúne tudo que fica exposto via HTTP: ``POST /mcp`` (JSON-RPC, ver
    ``McpServer.process_message``), o dashboard interativo (``GET /``, Fase
    7), a página de detalhe por backend (``GET /backend/{name}``), o console
    de logs ao vivo (``GET /api/logs/stream``), e as rotas de controle/
    observabilidade (``/api/servers``, ``/api/tools/size``,
    ``/api/config/backends``, ``/api/shutdown``, ``/health``).

    ``auth_token`` protege toda rota que expõe dado operacional (comandos,
    urls, logs) — ``None`` roda sem autenticação (uso local).

    Cada request HTTP ganha um ``request_id`` (UUID) vinculado via
    contextvars; todos os logs da mesma requisição carregam o mesmo id
    automaticamente (ver gateway.logging).
    """  
  
    def _maybe_open_browser() -> None:  
        """Abre o dashboard no navegador, salvo em modo headless.  
  
        Controlado por env var: ``MCP_GATEWAY_OPEN_BROWSER=false`` desliga;  
        porta e host vêm de ``MCP_GATEWAY_PORT``/``MCP_GATEWAY_HOST`` — as  
        mesmas env vars do bind do uvicorn (ver ``_dashboard_browser_url``).  
        """  
        if os.environ.get("MCP_GATEWAY_OPEN_BROWSER", "true").strip().lower() in (  
            "0",  
            "false",  
            "no",  
        ):  
            return  
        url = _dashboard_browser_url(auth_token)  
  
        def _open() -> None:  
            time.sleep(0.6)  # dá tempo do uvicorn começar a aceitar conexões  
            try:  
                webbrowser.open(url)  
            except Exception:  
                logger.info("dashboard_auto_open_failed", url=url)  
  
        threading.Thread(target=_open, daemon=True).start()  
  
    @asynccontextmanager  
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:  
        """Startup do Gateway: liga o console de logs ao loop e abre o dashboard.  
  
        Substitui o antigo ``@app.on_event("startup")`` (deprecado no FastAPI):  
        o código antes do ``yield`` roda no startup.  
        """  
        log_broadcaster.bind_loop(asyncio.get_running_loop())  
        _maybe_open_browser()  
        yield  
  
    app = FastAPI(  
        title="MCP Gateway",  
        version=APP_VERSION,  
        docs_url=None,  
        redoc_url=None,  
        openapi_url=None,  
        lifespan=lifespan,  
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
        """Resposta 401 padrão com o header ``WWW-Authenticate: Bearer``."""
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
        """Dashboard interativo do Gateway (Fase 7) — ver ``_render_dashboard``.

        MESMA auth do /api/servers: expõe detalhes operacionais (comandos,
        urls). Como navegador não declara Bearer, aceita ``/?token=<token>``
        quando auth está configurada — o header, quando presente, vence.

        O token que o próprio navegador usou para passar na auth (header ou
        query) é reembutido no HTML como constante JS — é o mesmo token que o
        usuário já digitou/colou na URL, nunca um segredo novo exposto.
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
        effective_token = (
            query_token
            if query_token is not None
            else (auth_token if request.headers.get("authorization") else None)
        )
        # Endpoint MCP agregado: o mesmo host:porta que o navegador já usou pra
        # abrir o dashboard, com o path /mcp — não depende de main.py repassar
        # host/porta a create_app (não repassa hoje).
        gw_endpoint = f"{request.url.scheme}://{request.url.netloc}/mcp"
        return HTMLResponse(
            content=_render_dashboard(
                mcp_server.backend_manager.health_summary(),
                mcp_server.backend_manager.server_details(),
                token=effective_token,
                gw_endpoint=gw_endpoint,
            )
        )

    @app.get("/backend/{name}", response_class=HTMLResponse)
    async def backend_detail(name: str, request: Request) -> Response:
        """Página de detalhe de um backend (Fase 7 — clique no card).

        MESMA auth do dashboard (aceita ``?token=``). 404 simples (HTML) se o
        backend não existe — pode ter sido removido entre o clique no card e
        o carregamento da página (ex.: outra aba removeu).
        """
        query_token = request.query_params.get("token")
        if not _is_authorized(request, token_override=query_token):
            logger.info("http_auth_rejected", route=f"/backend/{name}")
            return _unauthorized()
        effective_token = (
            query_token
            if query_token is not None
            else (auth_token if request.headers.get("authorization") else None)
        )

        state_details = [
            s for s in mcp_server.backend_manager.server_details() if s["name"] == name
        ]
        if not state_details:
            return HTMLResponse(
                status_code=404,
                content=(
                    "<!DOCTYPE html><html lang='pt-BR'><head><meta charset='utf-8'>"
                    "<title>404</title></head><body style='font-family:system-ui;"
                    "background:#0f1117;color:#e6e8ef;padding:2rem'>"
                    f"<h1>Backend '{html.escape(name)}' não encontrado</h1>"
                    "<p><a href='/' style='color:#5b8cff'>← Voltar ao dashboard</a></p>"
                    "</body></html>"
                ),
            )

        tools, resources, prompts = mcp_server.backend_manager.registries
        entries = {
            "tools": [e for e in tools.list_all() if e.backend == name],
            "resources": [e for e in resources.list_all() if e.backend == name],
            "prompts": [e for e in prompts.list_all() if e.backend == name],
        }
        return HTMLResponse(content=_render_backend_detail(state_details[0], entries, token=effective_token))

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
        except Exception as exc:
            logger.exception(
                "erro inesperado na rota de controle",
                backend=name,
                action=action,
                error=str(exc),
            )
            return JSONResponse(status_code=500, content={"detail": "Internal error"})
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

    @app.get("/api/servers/{name}/history")
    async def backend_history(name: str, request: Request) -> JSONResponse:
        """Histórico de status/latência de um backend (Fase 8).

        Uma amostra por ciclo do Health Monitor (``BackendManager.
        history_for``), em memória — reinicia com o Gateway. Lista vazia
        (não erro) quando o backend nunca passou por um ciclo ainda, ex.:
        acabou de ser adicionado.
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route=f"/api/servers/{name}/history")
            return _unauthorized()
        return JSONResponse(content={"samples": mcp_server.backend_manager.history_for(name)})

    @app.delete("/api/servers/{name}")
    async def remove_backend_route(name: str, request: Request) -> JSONResponse:
        """Remove um backend por completo (Fase 7 — botão × do card).

        Diferente de ``disable`` (mantém no config em estado neutro), isto:
        (1) para o client e apaga o backend da memória do Gateway
        (``BackendManager.remove_backend``); (2) remove a entrada do
        ``config.json`` em disco (escrita atômica, mesmo padrão de
        ``POST /api/config/backends``) — sem isso o backend voltaria no
        próximo restart do Gateway. Não usa ``_control_route`` porque aquele
        helper assume que o backend AINDA existe em ``_states`` depois da
        operação (verdade para disable/enable/restart, falso aqui).

        Recusa remover o ÚLTIMO backend (409): a ``GatewayConfig`` exige ao
        menos um backend no boot, então gravar uma lista vazia deixaria o
        config.json num estado que o Gateway não sobe — a remoção é bloqueada
        antes de qualquer efeito (memória ou disco).
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route=f"/api/servers/{name}")
            return _unauthorized()
        # Guarda ANTES de qualquer efeito (memória ou disco): a GatewayConfig
        # EXIGE ao menos um backend (model_validator). Remover o último
        # deixaria o config.json num estado que o boot seguinte rejeita — e,
        # sem hot-reload, o operador teria que editar o JSON à mão para
        # recuperar. O critério é o DISCO (fonte do boot); se ele estiver
        # ilegível, a guarda não bloqueia — o fluxo antigo (remoção + aviso
        # de escrita falha) preserva o caminho de recuperação pelo painel.
        config_path = os.environ.get("MCP_GATEWAY_CONFIG", "config/config.json")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw_disk = json.load(f)
            disk_backends = raw_disk.get("backends", []) if isinstance(raw_disk, dict) else []
            if len(disk_backends) == 1 and disk_backends[0].get("name") == name:
                return JSONResponse(
                    status_code=409,
                    content={
                        "detail": (
                            f"'{name}' é o último backend — o Gateway exige ao "
                            "menos um. Adicione outro antes de remover este."
                        )
                    },
                )
        except (OSError, json.JSONDecodeError):
            pass
        try:
            await mcp_server.backend_manager.remove_backend(name)
        except BackendError as exc:
            return _backend_error_response(exc)
        except Exception as exc:
            logger.exception("erro inesperado removendo backend", backend=name, error=str(exc))
            return JSONResponse(status_code=500, content={"detail": "Internal error"})
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw_config = json.load(f)
            backends = raw_config.get("backends", [])
            new_backends = [b for b in backends if b.get("name") != name]
            if len(new_backends) != len(backends):
                raw_config["backends"] = new_backends
                tmp_path = f"{config_path}.tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(raw_config, f, indent=2, ensure_ascii=False)
                    f.write("\n")
                os.replace(tmp_path, config_path)
        except (OSError, json.JSONDecodeError) as exc:
            # O backend já foi removido em memória (passo 1 não é desfeito);
            # só avisa que o arquivo em disco não pôde ser atualizado.
            logger.warning("backend_remove_config_write_failed", backend=name, error=str(exc))
            return JSONResponse(
                content={
                    "detail": f"'{name}' removido do Gateway, mas não foi possível atualizar"
                    f" {config_path}: {exc}. Ele pode voltar se o Gateway reiniciar."
                }
            )
        logger.info("backend_removed_via_dashboard", backend=name)
        return JSONResponse(content={"detail": f"'{name}' removido do Gateway e de {config_path}."})

    _SETTINGS_FIELDS = (
        "auth_token",
        "max_payload_bytes",
        "health_check_interval_seconds",
        "backend_request_timeout_seconds",
        "auto_restart",
        "max_restart_attempts",
        "session_ttl_seconds",
    )
    """Campos de config.json editáveis pela página de Configurações (Fase 8).

    Deliberadamente NÃO inclui ``backends`` (isso é o painel "Adicionar MCP" +
    o botão de remover, que já têm seu próprio fluxo com start/stop ao vivo).
    Nenhum destes campos é aplicado a quente — todos exigem reiniciar o
    Gateway, igual a editar o config.json na mão hoje.
    """

    @app.get("/api/config/settings")
    async def get_settings(request: Request) -> JSONResponse:
        """Devolve os campos de config.json editáveis pela página de Configurações."""
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/config/settings")
            return _unauthorized()
        config_path = os.environ.get("MCP_GATEWAY_CONFIG", "config/config.json")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw_config = json.load(f)
        except FileNotFoundError:
            return JSONResponse(
                status_code=500, content={"detail": f"config não encontrado em '{config_path}'"}
            )
        except json.JSONDecodeError as exc:
            return JSONResponse(
                status_code=500, content={"detail": f"config.json inválido: {exc}"}
            )
        return JSONResponse(content={key: raw_config.get(key) for key in _SETTINGS_FIELDS})

    @app.put("/api/config/settings")
    async def update_settings(request: Request) -> JSONResponse:
        """Grava os campos globais (não-backend) no config.json (Fase 8).

        Valida o resultado inteiro via ``GatewayConfig.model_validate`` antes
        de gravar — os mesmos limites/tipos que o ``main.py`` exige no boot
        (ex.: ``max_restart_attempts >= 1``) são aplicados aqui, então um
        valor inválido nunca chega a virar um config.json que falharia ao
        subir. Escrita atômica, mesmo padrão das outras rotas de config.
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/config/settings")
            return _unauthorized()
        try:
            payload = await request.json()
        except Exception:
            return JSONResponse(status_code=400, content={"detail": "JSON inválido"})
        if not isinstance(payload, dict):
            return JSONResponse(status_code=422, content={"detail": "corpo deve ser um objeto JSON"})

        config_path = os.environ.get("MCP_GATEWAY_CONFIG", "config/config.json")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw_config = json.load(f)
        except FileNotFoundError:
            return JSONResponse(
                status_code=500, content={"detail": f"config não encontrado em '{config_path}'"}
            )
        except json.JSONDecodeError as exc:
            return JSONResponse(
                status_code=500, content={"detail": f"config.json inválido: {exc}"}
            )

        updated = dict(raw_config)
        for key in _SETTINGS_FIELDS:
            if key in payload:
                updated[key] = payload[key]

        try:
            GatewayConfig.model_validate(updated)
        except ValidationError as exc:
            return JSONResponse(
                status_code=422, content={"detail": f"configuração inválida: {exc}"}
            )

        tmp_path = f"{config_path}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(updated, f, indent=2, ensure_ascii=False)
                f.write("\n")
            os.replace(tmp_path, config_path)
        except OSError as exc:
            return JSONResponse(
                status_code=500, content={"detail": f"falha ao gravar config: {exc}"}
            )

        logger.info("settings_updated_via_dashboard")
        return JSONResponse(
            content={"detail": "Configurações salvas. Reinicie o Gateway para aplicar."}
        )

    @app.get("/api/config/backup")
    async def config_backup(request: Request) -> Response:
        """Devolve o ``config.json`` bruto (Fase 8 — botão "Baixar backup").

        Conteúdo idêntico ao arquivo em disco, sem reformatação — o backup
        baixado é byte-a-byte o que estava salvo no momento do clique.
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/config/backup")
            return _unauthorized()
        config_path = os.environ.get("MCP_GATEWAY_CONFIG", "config/config.json")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw_text = f.read()
        except OSError as exc:
            return JSONResponse(
                status_code=500, content={"detail": f"falha ao ler config: {exc}"}
            )
        return Response(content=raw_text, media_type="application/json")

    @app.post("/api/config/restore")
    async def config_restore(request: Request) -> JSONResponse:
        """Restaura o ``config.json`` inteiro a partir de um backup (Fase 8).

        Valida o arquivo inteiro via ``GatewayConfig.model_validate`` ANTES de
        gravar qualquer coisa — um backup corrompido ou de um schema
        incompatível nunca chega a sobrescrever o config.json real. Escrita
        atômica. Não sobe/derruba backend nenhum em tempo real (diferente do
        "Adicionar MCP") — restaurar troca o arquivo; aplicar exige reiniciar
        o Gateway, como qualquer outra edição de config global.
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/config/restore")
            return _unauthorized()
        try:
            payload = await request.json()
        except Exception:
            return JSONResponse(status_code=400, content={"detail": "JSON inválido"})

        try:
            GatewayConfig.model_validate(payload)
        except ValidationError as exc:
            return JSONResponse(
                status_code=422, content={"detail": f"backup inválido, nada foi gravado: {exc}"}
            )

        config_path = os.environ.get("MCP_GATEWAY_CONFIG", "config/config.json")
        tmp_path = f"{config_path}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
                f.write("\n")
            os.replace(tmp_path, config_path)
        except OSError as exc:
            return JSONResponse(
                status_code=500, content={"detail": f"falha ao gravar config: {exc}"}
            )

        logger.warning("config_restored_via_dashboard", backends=len(payload.get("backends", [])))
        return JSONResponse(
            content={"detail": "Config restaurado. Reinicie o Gateway para aplicar."}
        )

    @app.get("/api/import/claude-desktop/detect")
    async def import_claude_detect(request: Request) -> JSONResponse:
        """Busca o claude_desktop_config.json nos caminhos conhecidos (Fase 8).

        Cobre instalação clássica e MSIX no Windows, além de macOS/Linux — ver
        ``scripts/import_claude_desktop_config.find_claude_desktop_config``.
        Só informa se achou e onde; não lê nem converte nada ainda (isso é o
        ``POST`` desta mesma família de rotas).
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/import/claude-desktop/detect")
            return _unauthorized()
        try:
            from scripts.import_claude_desktop_config import find_claude_desktop_config
        except ImportError as exc:
            return JSONResponse(
                status_code=500,
                content={"detail": f"script de importação indisponível: {exc}"},
            )
        found = find_claude_desktop_config()
        return JSONResponse(content={"found": found is not None, "path": str(found) if found else None})

    @app.post("/api/import/claude-desktop")
    async def import_claude_desktop(request: Request) -> JSONResponse:
        """Importa o claude_desktop_config.json pro config.json do Gateway (Fase 8).

        Reaproveita ``import_config`` do script de linha de comando (mesma
        lógica, mesmos avisos) — a diferença é o destino: em vez de gravar um
        arquivo separado pra revisão manual, MESCLA os backends convertidos
        direto no ``config.json`` real, pulando qualquer nome que já exista
        (em memória ou em disco — mesma checagem de duplicata do "Adicionar
        MCP"). Nunca sobrescreve um backend existente silenciosamente.

        Não sobe nada em tempo real (diferente do "Adicionar MCP") — são
        potencialmente vários backends de uma vez, então a aplicação fica
        pra um restart do Gateway, com o operador revisando os avisos antes.

        MESMO portão do restore/settings: o resultado mesclado passa por
        ``GatewayConfig.model_validate`` ANTES de tocar o disco — ``import_config``
        não valida contra o schema (ex.: ``url`` sem host, ``headers`` não-string
        passam direto pelo ``convert_entry``), então sem isso uma entrada
        importada podia envenenar o config.json e quebrar o boot seguinte.
        Bloqueio = 422 e nada gravado.
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/import/claude-desktop")
            return _unauthorized()
        try:
            payload = await request.json()
        except Exception:
            return JSONResponse(status_code=400, content={"detail": "JSON inválido"})
        if not isinstance(payload, dict):
            payload = {}

        try:
            from scripts.import_claude_desktop_config import (
                find_claude_desktop_config,
                import_config,
            )
        except ImportError as exc:
            return JSONResponse(
                status_code=500,
                content={"detail": f"script de importação indisponível: {exc}"},
            )

        path_raw = payload.get("path")
        source_path = Path(path_raw) if path_raw else find_claude_desktop_config()
        if source_path is None:
            return JSONResponse(
                status_code=404,
                content={
                    "detail": (
                        "não encontrei o claude_desktop_config.json em nenhum caminho"
                        " conhecido — informe o caminho manualmente"
                    )
                },
            )
        if not source_path.exists():
            return JSONResponse(
                status_code=404, content={"detail": f"arquivo não encontrado: {source_path}"}
            )

        prefix = payload.get("prefix") or ""
        try:
            imported, warnings = import_config(source_path, prefix=str(prefix))
        except ValueError as exc:
            return JSONResponse(status_code=422, content={"detail": str(exc)})

        config_path = os.environ.get("MCP_GATEWAY_CONFIG", "config/config.json")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw_config = json.load(f)
        except FileNotFoundError:
            return JSONResponse(
                status_code=500, content={"detail": f"config não encontrado em '{config_path}'"}
            )
        except json.JSONDecodeError as exc:
            return JSONResponse(status_code=500, content={"detail": f"config.json inválido: {exc}"})

        existing_backends = raw_config.setdefault("backends", [])
        existing_names = {s["name"] for s in mcp_server.backend_manager.server_details()} | {
            b.get("name") for b in existing_backends
        }
        added: list[str] = []
        skipped: list[str] = []
        for backend in imported["backends"]:
            if backend["name"] in existing_names:
                skipped.append(backend["name"])
                continue
            existing_backends.append(backend)
            existing_names.add(backend["name"])
            added.append(backend["name"])

        if added:
            # MESMO portão do restore/settings: o resultado mesclado tem que
            # passar pelo MESMO schema que o load_config exige no boot, ANTES
            # de gravar — sem isso, entrada importada que o convert_entry não
            # consegue checar (url sem host, headers não-string...) iria para
            # o config.json real e quebraria o próximo boot.
            try:
                GatewayConfig.model_validate(raw_config)
            except ValidationError as exc:
                return JSONResponse(
                    status_code=422,
                    content={
                        "detail": (
                            "importação bloqueada: o config resultante não passa no "
                            f"schema do boot, nada foi gravado: {exc}"
                        )
                    },
                )
            tmp_path = f"{config_path}.tmp"
            try:
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(raw_config, f, indent=2, ensure_ascii=False)
                    f.write("\n")
                os.replace(tmp_path, config_path)
            except OSError as exc:
                return JSONResponse(
                    status_code=500, content={"detail": f"falha ao gravar config: {exc}"}
                )

        logger.info(
            "claude_desktop_imported", source=str(source_path), added=len(added), skipped=len(skipped)
        )
        detail = f"{len(added)} backend(s) importado(s) de {source_path}."
        if skipped:
            detail += f" {len(skipped)} pulado(s) por nome já existente: {', '.join(skipped)}."
        if added:
            detail += " Reinicie o Gateway para aplicar."
        return JSONResponse(
            content={
                "detail": detail,
                "added": added,
                "skipped": skipped,
                "warnings": warnings,
            }
        )

    @app.post("/api/config/backends")
    async def add_backend_config(request: Request) -> JSONResponse:
        """Adiciona um backend ao ``config.json`` em disco e sobe ele na hora
        (Fase 7 — painel "Adicionar MCP" do dashboard).

        Duas etapas, nessa ordem: (1) grava a entrada no arquivo de config —
        escrita atômica (``.tmp`` + ``os.replace``), então o restart do
        Gateway já nasce vendo o backend novo; (2) chama
        ``BackendManager.add_backend()`` pra também deixá-lo rodando
        imediatamente, sem precisar reiniciar nada. Se o passo 2 falhar (ex.:
        comando/URL não responde agora), o backend fica registrado como
        ``offline`` e o HealthMonitor assume o restart sozinho — o passo 1
        (gravação em disco) nunca é desfeito por uma falha no passo 2.

        Antes de qualquer efeito (disco ou memória), a entrada montada é
        validada contra o PRÓPRIO ``BackendConfig`` — o mesmo schema que o
        boot usa é a fonte única de verdade (a checagem leve do payload só
        produz mensagens amigáveis, não decide). Sem isso, uma entrada que o
        schema rejeita podia ser gravada no ``config.json`` e quebrar o boot
        seguinte — divergência de validação entre rota e schema, a mesma raiz
        dos achados de nome (regex ASCII vs. ``c.isalnum()``), só que em
        outros campos: ex.: ``url="http://"`` (sem host) passava na checagem
        leve por ``startswith`` e era rejeitada pelo schema, que exige
        scheme E host.

        O arquivo de config é resolvido pela MESMA env var e default do
        ``main.py`` (``MCP_GATEWAY_CONFIG`` -> ``config/config.json``), pra o
        painel sempre achar o arquivo que o Gateway realmente carregou no boot.

        Duplicata é checada contra dois lugares: os backends já carregados em
        memória (``server_details()``) E os que já estão no arquivo mas ainda
        não foram carregados.
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/config/backends")
            return _unauthorized()

        try:
            payload = await request.json()
        except Exception:
            return JSONResponse(status_code=400, content={"detail": "JSON inválido"})

        error = _validate_new_backend_payload(payload)
        if error:
            return JSONResponse(status_code=422, content={"detail": error})

        name = payload["name"].strip()
        config_path = os.environ.get("MCP_GATEWAY_CONFIG", "config/config.json")

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw_config = json.load(f)
        except FileNotFoundError:
            return JSONResponse(
                status_code=500,
                content={"detail": f"config não encontrado em '{config_path}'"},
            )
        except json.JSONDecodeError as exc:
            return JSONResponse(status_code=500, content={"detail": f"config.json inválido: {exc}"})

        backends = raw_config.setdefault("backends", [])
        in_memory_names = {s["name"] for s in mcp_server.backend_manager.server_details()}
        on_disk_names = {b.get("name") for b in backends}
        if name in in_memory_names or name in on_disk_names:
            return JSONResponse(
                status_code=409, content={"detail": f"já existe um backend chamado '{name}'"}
            )

        entry = _build_backend_entry(payload)
        try:
            backend_config = BackendConfig(**entry)
        except ValidationError as exc:
            # Fonte única de verdade: o MESMO schema que o load_config usa no
            # boot decide aqui, ANTES da gravação — nenhum caminho desta rota
            # deixa no config.json uma entrada que o próximo boot rejeitaria.
            return JSONResponse(
                status_code=422,
                content={"detail": f"entrada rejeitada pelo schema do config: {exc}"},
            )
        backends.append(entry)

        tmp_path = f"{config_path}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(raw_config, f, indent=2, ensure_ascii=False)
                f.write("\n")
            os.replace(tmp_path, config_path)
        except OSError as exc:
            return JSONResponse(
                status_code=500, content={"detail": f"falha ao gravar config: {exc}"}
            )

        logger.info(
            "backend_added_to_config", backend=name, backend_type=entry.get("type", "stdio")
        )

        live_note = ""
        try:
            # backend_config já nasceu validado contra o schema (antes da
            # gravação em disco) — aqui é só o start ao vivo.
            await mcp_server.backend_manager.add_backend(backend_config)
            live_status = mcp_server.backend_manager.status_of(name).value
            live_note = (
                " Já está rodando (running)."
                if live_status == "running"
                else f" Adicionado, mas ainda não conectou (status: {live_status}) — o Gateway vai tentar de novo sozinho."
            )
        except ValueError as exc:
            live_note = f" Gravado no config, mas não subiu agora: {exc}"
        except Exception:
            logger.exception("backend_live_start_unexpected_error", backend=name)
            live_note = " Gravado no config, mas houve um erro inesperado ao tentar subir agora — confira o console."

        return JSONResponse(
            content={
                "detail": f"'{name}' adicionado a {config_path}.{live_note}",
                "backend": entry,
            }
        )

    @app.post("/api/shutdown")
    async def shutdown_gateway(request: Request) -> JSONResponse:
        """Encerra o processo do Gateway (Fase 7 — botão "Sair do MCP").

        Não mata o processo diretamente (``os.kill``/``sys.exit`` aqui
        derrubariam a conexão HTTP no meio da resposta e, no Windows,
        ``SIGTERM`` via ``os.kill`` ignora os handlers do Python e mata
        na marra). Em vez disso, só marca ``uvicorn.Server.should_exit =
        True`` — a referência ao ``Server`` é guardada em
        ``app.state.uvicorn_server`` pelo ``main.py`` logo após criá-lo.
        Isso entra no MESMO caminho de shutdown gracioso do Ctrl+C:
        ``server.serve()`` retorna e o ``finally`` do ``main()`` para o
        Health Monitor, o Session Purger e cada backend (sem órfãos) antes
        do processo sair de verdade — então a resposta desta rota chega ao
        navegador antes do processo realmente terminar.
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/shutdown")
            return _unauthorized()
        server = getattr(request.app.state, "uvicorn_server", None)
        if server is None:
            # Só acontece se create_app for usado fora do main.py padrão
            # (ex.: em testes) sem a integração de shutdown.
            return JSONResponse(
                status_code=501,
                content={"detail": "shutdown não disponível neste processo"},
            )
        logger.warning("gateway_shutdown_requested_via_dashboard")
        # Dar tempo pra resposta HTTP sair antes do uvicorn parar.
        # Sem isso, o client recebe ConnectionResetError no browser.
        asyncio.get_event_loop().call_later(0.1, setattr, server, "should_exit", True)
        return JSONResponse(content={"detail": "Encerrando o Gateway…"})

    @app.get("/api/tools/size")
    async def tools_size(request: Request) -> JSONResponse:
        """Diagnóstico (Fase 5): tamanho do tools/list (chars/tokens aprox.).

        Mesma auth do /api/servers: os payloads das tools podem revelar
        detalhes da superfície exposta. Aceita ``Mcp-Session-Id`` para medir o
        tools/list DE UMA SESSÃO filtrada — é o que o operador usa para decidir
        se o filtro seletivo vale a pena (e quanto economiza). O header cru é
        normalizado antes de medir/consultar a sessão (valor gigante/inválido
        vira "sem sessão", nunca chave de dict).
        """
        if not _is_authorized(request):
            logger.info("http_auth_rejected", route="/api/tools/size")
            return _unauthorized()
        return JSONResponse(
            content=mcp_server.tools_list_size(
                normalize_session_id(request.headers.get(SESSION_HEADER))
            )
        )

    @app.get("/api/logs/stream")
    async def logs_stream(request: Request) -> Response:
        """Console de logs ao vivo do dashboard (Fase 7) — Server-Sent Events.

        Mesma exceção de auth que a rota ``/`` (aceita ``?token=`` além do
        Bearer): quem consome esta rota é o próprio ``EventSource`` do
        navegador carregado pelo dashboard, e a API nativa de EventSource não
        permite setar headers customizados — só a query string chega até
        aqui. O evento nunca inclui o token em si (é o log do Gateway, não a
        URL da request).

        O buffer recente é reenviado primeiro (replay), pra quem acabou de
        abrir o dashboard já ver contexto em vez de tela vazia; depois o loop
        entrega eventos ao vivo com heartbeat periódico para manter a conexão
        viva atrás de proxies.
        """
        if not _is_authorized(request, token_override=request.query_params.get("token")):
            logger.info("http_auth_rejected", route="/api/logs/stream")
            return _unauthorized()  # type: ignore[return-value]

        # Rate limit por IP: 10 conn/min
        client_ip = request.client.host if request.client else "unknown"
        if not _log_stream_limiter.allow(client_ip):
            logger.warning("log_stream_rate_limited", client_ip=client_ip)
            return JSONResponse(
                status_code=429,
                content={"detail": "rate limit excedido (10/min)"},
            )

        try:
            queue, replay = await log_broadcaster.subscribe()
        except RuntimeError as exc:
            logger.warning("log_stream_subscriber_limit", error=str(exc))
            return JSONResponse(
                status_code=503,
                content={"detail": str(exc)},
            )

        async def event_source() -> Any:
            try:
                for line in replay:
                    yield f"data: {line}\n\n"
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        line = await asyncio.wait_for(queue.get(), timeout=15.0)
                        yield f"data: {line}\n\n"
                    except asyncio.TimeoutError:
                        yield ": heartbeat\n\n"
            finally:
                log_broadcaster.unsubscribe(queue)

        return StreamingResponse(
            event_source(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    async def _process_request(request: Request) -> Response:
        """Executa o pipeline de validação de transporte do POST /mcp.

        Ordem: (1) autenticação — aceita Bearer no header OU ``?token=`` na
        query, pois ``cmd.exe`` no Windows corrompe headers com espaço em
        argumentos de ``npx``; o valor da query nunca é logado (nenhum ponto
        deste módulo emite a URL, e o access log do uvicorn fica desligado em
        ``main.py``); (2) Content-Type application/json (415); (3) tamanho do
        payload dentro de ``max_payload_bytes`` (413, checado no header
        ``content-length`` e no streaming do corpo); (4) parse do JSON
        (ParseError). Batch não é suportado: corpo não-dict vira INVALID_REQUEST.
        """
        if not _is_authorized(request, token_override=request.query_params.get("token")):
            logger.info("http_auth_rejected")
            return JSONResponse(
                status_code=401,
                content={
                    "detail": "Autenticação necessária: header 'Authorization: Bearer <token>'"
                },
                headers={"WWW-Authenticate": "Bearer"},
            )

        content_type = request.headers.get("content-type", "")
        media_type = content_type.split(";", 1)[0].strip().lower()
        if media_type != "application/json":
            return JSONResponse(
                status_code=415,
                content={"detail": "Content-Type deve ser application/json"},
            )

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
                    content={"detail": f"Payload excede o limite de {max_payload_bytes} bytes"},
                )

        body_parts: list[bytes] = []
        body_size = 0
        async for chunk in request.stream():
            body_size += len(chunk)
            if body_size > max_payload_bytes:
                logger.warning("http_payload_too_large", max_bytes=max_payload_bytes)
                return JSONResponse(
                    status_code=413,
                    content={"detail": f"Payload excede o limite de {max_payload_bytes} bytes"},
                )
            body_parts.append(chunk)
        body = b"".join(body_parts)
        if body_size > max_payload_bytes:
            logger.warning("http_payload_too_large", max_bytes=max_payload_bytes)
            return JSONResponse(
                status_code=413,
                content={"detail": f"Payload excede o limite de {max_payload_bytes} bytes"},
            )

        try:
            raw_body: Any = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return JSONResponse(
                status_code=400,
                content=make_error(None, PARSE_ERROR, "Parse error"),
            )

        if not isinstance(raw_body, dict):
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
                raw_body,
                session_id=normalize_session_id(request.headers.get(SESSION_HEADER)),
            )
        except Exception as exc:
            logger.exception("erro inesperado processando mensagem JSON-RPC", error=str(exc))
            return JSONResponse(
                status_code=200,
                content=make_error(None, INTERNAL_ERROR, "Internal error"),
            )
        if response is None:
            return Response(status_code=202)
        return JSONResponse(status_code=200, content=response)

    @app.post("/mcp")
    async def mcp_endpoint(request: Request) -> Response:
        """Endpoint JSON-RPC principal do Gateway.

        Vincula um ``request_id`` (UUID) via contextvars pra correlacionar
        todos os logs da requisição, mede a duração e emite
        ``http_request_completed`` ao final. Erros inesperados viram 500 sem
        vazar detalhes; o ``clear_contextvars`` no ``finally`` garante que o
        id não vaze para a próxima task.
        """
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
