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
import secrets
import threading
import time
import uuid
import webbrowser
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

from gateway.config import DEFAULT_MAX_PAYLOAD_BYTES, BackendConfig
from gateway.errors import (
    BackendError,
    BackendNotFoundError,
    BackendStateConflictError,
)
from gateway.log_stream import log_broadcaster
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
  position: sticky; top: 0; background: var(--bg); z-index: 5;  
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
#add-mcp-form label {  
  display: block; font-size: .8rem; color: var(--muted); margin-top: .7rem;  
}  
#add-mcp-form input, #add-mcp-form select, #add-mcp-form textarea {  
  width: 100%; margin-top: .25rem; background: var(--panel-2); border: 1px solid var(--border);  
  color: var(--text); border-radius: .35rem; padding: .4rem .5rem; font-size: .85rem;  
  font-family: inherit;  
}  
#add-mcp-form textarea { font-family: ui-monospace, monospace; resize: vertical; }  
.type-fields.hidden { display: none; }  
.hint { color: var(--muted); font-size: .74rem; margin-top: .8rem; }  
.error { color: var(--err); font-size: .78rem; margin-top: .5rem; }  
.error.hidden { display: none; }  
.modal-actions { display: flex; justify-content: flex-end; gap: .5rem; margin-top: 1rem; }  
.modal-actions button { padding: .4rem .9rem; border-radius: .4rem; font-size: .82rem; cursor: pointer; }  
#cancel-add-mcp { background: var(--panel-2); border: 1px solid var(--border); color: var(--text); }  
  
.conn-banner {  
  background: var(--err); color: #fff; text-align: center; font-size: .85rem;  
  padding: .45rem; font-weight: 600;  
}  
.conn-banner.hidden { display: none; }  
  
.btn-secondary {  
  background: var(--panel-2); border: 1px solid var(--border); color: var(--text);  
  padding: .4rem .7rem; border-radius: .4rem; font-size: .8rem; cursor: pointer;  
}  
.btn-secondary:hover { border-color: var(--accent); }  
  
.switch { display: inline-flex; align-items: center; gap: .35rem; font-size: .8rem; color: var(--muted); cursor: pointer; }  
.switch input { accent-color: var(--accent); }  
  
body.readonly-mode .actions, body.readonly-mode #open-add-mcp { display: none; }  
  
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
"""


def _render_dashboard(
    summary: dict[str, Any],
    servers: list[dict[str, Any]],
    *,
    token: str | None,
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
    """
    status = str(summary.get("status", "?"))
    status_safe = html.escape(status)
    totals = summary.get("tools_count", 0)
    res_count = summary.get("resources_count", 0)
    prompt_count = summary.get("prompts_count", 0)
    cards_html = _render_backend_cards(servers)
    token_js = json.dumps(token)
    return f"""<!DOCTYPE html>  
<html lang="pt-BR">  
<head>  
<meta charset="utf-8">  
<meta name="viewport" content="width=device-width, initial-scale=1">  
<title>MCP Gateway — Dashboard</title>  
<style>{_DASHBOARD_STYLE}</style>  
</head>  
<body>  
<div id="conn-banner" class="conn-banner hidden">⚠ Conexão perdida com o Gateway — tentando reconectar…</div>  
<header>  
  <h1>MCP Gateway</h1>  
  <span id="status-pill" class="pill pill-{status_safe}"><span class="dot"></span>{status_safe}</span>  
  <span class="stat" id="totals-stat">Tools: {totals} · Resources: {res_count} · Prompts: {prompt_count}</span>  
  <span class="grow"></span>  
  <label class="switch"><input type="checkbox" id="readonly-toggle"><span>Somente leitura</span></label>  
  <button id="export-snapshot" class="btn-secondary" title="Baixar JSON com /health + /api/servers + /api/tools/size">Exportar snapshot</button>  
  <button id="open-add-mcp" class="btn-primary">+ Adicionar MCP</button>  
</header>  
<main>  
  <h2>Backends</h2>  
  <div id="backends-grid">{cards_html}</div>  
  
  <h2>Console (logs ao vivo)</h2>  
  <div id="console-wrap">  
    <div id="console-toolbar">  
      <span id="console-status">conectando…</span>  
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
      <button id="console-clear-filter" class="hidden">Limpar filtro request_id</button>  
      <button id="console-pause">Pausar</button>  
      <button id="console-clear">Limpar</button>  
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
  
<script>  
const GW_TOKEN = {token_js};  
const AUTH_HEADERS = GW_TOKEN ? {{"Authorization": "Bearer " + GW_TOKEN}} : {{}};  
  
function fmtMeta(s) {{  
  const bits = [];  
  if (s.type) bits.push(s.type);  
  if (s.url) bits.push(s.url);  
  if (s.command) bits.push((s.command + " " + (s.args || []).join(" ")).trim());  
  return bits.join(" · ");  
}}  
  
// last_restart_at do servidor é um timestamp MONOTÔNICO (não é hora real) —  
// não dá pra converter direto em data. Em vez disso, guardamos QUANDO (hora  
// real local) vimos esse valor mudar pela primeira vez, e humanizamos a  
// partir daí. Funciona enquanto o dashboard ficou aberto desde o restart;  
// se a página abriu depois, mostra "recente" na primeira vez que aparece.  
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
  
function renderCard(s) {{  
  const st = (s.status || "?").toLowerCase();  
  const disabled = st === "disabled";  
  const canDisable = st !== "disabled";  
  const canEnable = st === "disabled";  
  const restartLabel = humanizeRestart(s.name, s.last_restart_at);  
  return `  
    <div class="card" data-name="${{s.name}}">  
      <div class="card-head">  
        <span class="name">${{s.name}}</span>  
        <span class="badge badge-${{st}}">${{st}}</span>  
      </div>  
      <div class="meta">${{fmtMeta(s)}}</div>  
      <div class="counts">  
        <span>Tools: <b>${{s.tools_count ?? 0}}</b></span>  
        <span>Res: <b>${{s.resources_count ?? 0}}</b></span>  
        <span>Prompts: <b>${{s.prompts_count ?? 0}}</b></span>  
        <span>Falhas: <b>${{s.consecutive_failures ?? 0}}</b></span>  
      </div>  
      ${{restartLabel ? `<div class="meta">Último restart: ${{restartLabel}}</div>` : ""}}  
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
  
function renderGrid(servers) {{  
  const grid = document.getElementById("backends-grid");  
  if (!servers.length) {{ grid.innerHTML = '<div class="empty">Nenhum backend configurado.</div>'; return; }}  
  let html = "";  
  for (const group of GROUPS) {{  
    const items = servers  
      .filter(s => group.statuses.includes((s.status || "").toLowerCase()))  
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
      pill.innerHTML = '<span class="dot"></span>' + h.status;  
      document.getElementById("totals-stat").textContent =  
        `Tools: ${{h.tools_count}} · Resources: ${{h.resources_count}} · Prompts: ${{h.prompts_count}}`;  
    }}  
    if (serversRes.ok) {{  
      const data = await serversRes.json();  
      diffAndToast(data.servers || []);  
      renderGrid(data.servers || []);  
    }}  
    fetchFailStreak = 0;  
  }} catch (e) {{  
    fetchFailStreak++;  
  }} finally {{  
    updateConnBanner();  
  }}  
}}  
  
document.getElementById("backends-grid").addEventListener("click", async (ev) => {{  
  const btn = ev.target.closest("button.act");  
  if (!btn) return;  
  const card = ev.target.closest(".card");  
  const name = card.dataset.name;  
  const action = btn.dataset.action;  
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
}});  
  
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
  
function updateErrorRate() {{  
  const now = Date.now();  
  const recentErrors = allEvents.filter(evt => {{  
    const lvl = (evt.level || "").toLowerCase();  
    if (lvl !== "error" && lvl !== "critical") return false;  
    const t = typeof evt.timestamp === "number" ? evt.timestamp * 1000 : new Date(evt.timestamp).getTime();  
    return !isNaN(t) && (now - t) <= 60000;  
  }}).length;  
  errorRateEl.textContent = `${{recentErrors}} erro${{recentErrors === 1 ? "" : "s"}}/min`;  
  errorRateEl.classList.toggle("hidden", recentErrors === 0);  
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

    Produz o mesmo shape que ``renderCard`` gera no JS, para o primeiro paint
    não ficar em branco antes do primeiro ``refresh``. Todo valor dinâmico
    passa por ``html.escape``.
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


def _validate_new_backend_payload(payload: Any) -> str | None:
    """Valida o corpo de ``POST /api/config/backends``.

    Espelha as regras que ``BackendConfig`` exige: ``stdio`` precisa de
    ``command`` (e ``args`` opcional como lista de strings); ``http``/``sse``
    precisam de ``url`` começando com ``http://`` ou ``https://``. O ``name``
    é obrigatório e restrito a letras, números, ``-`` e ``_``.

    Returns:
        A mensagem de erro (string) do primeiro problema encontrado, ou
        ``None`` se o payload for válido.
    """
    if not isinstance(payload, dict):
        return "corpo deve ser um objeto JSON"
    name = payload.get("name")
    if not isinstance(name, str) or not name.strip():
        return "campo 'name' é obrigatório"
    if not all(c.isalnum() or c in "-_" for c in name.strip()):
        return "'name' só pode ter letras, números, '-' e '_'"
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

    Produz o mesmo shape das entradas existentes: backends ``stdio`` NÃO
    carregam a chave ``type`` (igual ao ``backend-a`` do config original) e
    ``args`` só é incluído quando há argumentos não vazios; ``http``/``sse``
    carregam ``type`` e ``url``.
    """
    name = payload["name"].strip()
    btype = payload.get("type", "stdio")
    if btype == "stdio":
        entry: dict[str, Any] = {"name": name, "command": payload["command"].strip()}
        args = [a for a in (payload.get("args") or []) if a]
        if args:
            entry["args"] = args
        return entry
    return {"name": name, "type": btype, "url": payload["url"].strip()}


def create_app(  
    mcp_server: McpServer,  
    *,  
    auth_token: str | None = None,  
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,  
) -> FastAPI:  
    """...docstring inalterada..."""  
  
    def _maybe_open_browser() -> None:  
        """Abre o dashboard no navegador, salvo em modo headless.  
  
        Controlado por env var: ``MCP_GATEWAY_OPEN_BROWSER=false`` desliga;  
        ``MCP_GATEWAY_PORT`` informa a porta real do uvicorn (default 8080),  
        já que este módulo não sabe em que porta foi montado.  
        """  
        if os.environ.get("MCP_GATEWAY_OPEN_BROWSER", "true").strip().lower() in (  
            "0",  
            "false",  
            "no",  
        ):  
            return  
        port = os.environ.get("MCP_GATEWAY_PORT", "8080")  
        url = f"http://127.0.0.1:{port}/"  
        if auth_token:  
            url += f"?token={auth_token}"  
  
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
        """Dashboard read-only (server-rendered, sem JavaScript).

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
        return HTMLResponse(
            content=_render_dashboard(
                mcp_server.backend_manager.health_summary(),
                mcp_server.backend_manager.server_details(),
                token=effective_token,
            )
        )

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
            backend_config = BackendConfig(**entry)
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
    async def logs_stream(request: Request) -> StreamingResponse:
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

        queue, replay = await log_broadcaster.subscribe()

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
