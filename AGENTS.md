# AGENTS.md — Instruções para contribuir no Sentinel

Regras para qualquer contribuidor (humano ou agente). O código referencia este
documento em pontos-chave (ex.: a "regra de camadas do AGENT_INSTRUCTIONS" em
`gateway/server.py` — nome antigo, mesmo conteúdo). Roadmap de features vive
no [README](README.md), seção "Escopo e pendências".

## Comandos

```bash
python -m venv .venv
pip install -r requirements.txt

python -m pytest        # suíte completa (backends fake)
python -m ruff check .  # lint
python -m mypy          # type-checking (gateway + main.py + scripts)
```

CI (`.github/workflows/ci.yml`) roda os três com versões pinadas.

## Layout

- `main.py` — entrypoint: boot, config, hot-reload, shutdown gracioso
- `gateway/server.py` — camada de protocolo (JSON-RPC)
- `gateway/backend_manager.py` — ciclo de vida dos backends
- `gateway/clients/` — transporte: stdio / HTTP / SSE
- `gateway/registries/` — tools, resources, prompts (namespacing)
- `gateway/http_server.py` — dashboard, API REST, `POST /mcp`, stream de logs
- `gateway/health_monitor.py` — saúde, auto-restart, histórico em memória
- `gateway/sessions.py` — sessões, TTL, filtro seletivo
- `gateway/config.py` / `gateway/models.py` — schema e leitura validada do config
- `tests/fake_*.py` — backends MCP fake (subprocessos reais)

## Regras (vinculantes)

1. **Camadas**: acesso aos clients sempre via `BackendManager`, nunca direto a
   um `Client`. A camada de protocolo não conhece clients concretos.
2. **Dependências**: nenhuma nova sem necessidade clara — stdlib ou
   `requirements.txt` já existente.
3. **Testes**: só backends fake (`tests/fake_*.py`), nunca MCPs reais de
   terceiros. Comportamento novo entra com teste; correção de bug entra com
   teste de regressão. Rode a suíte antes de abrir mudança.
4. **Ciclo de vida**: registro atômico (se tools/resources/prompts falham no
   meio, desfaz os já feitos e para o client); nenhum processo/conexão órfão;
   `CancelledError` nunca é engolido — só `main.py` silencia, após cleanup.
5. **Segurança**: token sempre via `secrets.compare_digest`; nunca logar
   segredos; token só em `config/config.local.json` (gitignored; mesclado ao
   boot sobre o `config.json`).
6. **Logging**: tudo via `structlog`; access log do uvicorn desligado.
7. **Escopo**: nenhuma fase/transporte novo sem atualizar "Escopo e
   pendências" no README.
