# McpSentinel (MCP Gateway)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Lobby/router em Python que conecta e agrega todos os servidores MCP da máquina
em um único ponto de entrada HTTP: qualquer cliente MCP (Claude Desktop, Cursor,
agentes) enxerga vários backends como se fossem um só. O Gateway **inicia e
gerencia** os backends a partir de um arquivo de configuração explícito — não
há descoberta automática de portas.

- Agrega backends **stdio**, **HTTP** e **SSE** num único `POST /mcp`
  (namespacing automático `backend.tool`)
- Agrega também `resources` e `prompts`, não só tools
- Health monitor com auto-restart e backoff exponencial
- Filtro seletivo de backends por sessão (economia de contexto para agentes
  com modelos menores)
- Dashboard read-only, rotas de controle (`disable`/`enable`/`restart`) e
  importador de `claude_desktop_config.json`
- Autenticação Bearer opcional (header ou `?token=`), logging estruturado com
  `request_id`, graceful shutdown sem órfãos

**Status atual: Fase 5 concluída** — filtro seletivo de backends por sessão
(extensão `gateway/session/*`), controle manual de backends, dashboard e
importador. Testes: **258/258 passando**. O `SseClient` segue o padrão
HTTP+SSE do MCP (evento `endpoint` com URL literal), validado contra
servidores SSE reais. Veja o `ROADMAP.md` para o plano completo e o
`AGENT_INSTRUCTIONS.md` para as regras de conduta.

## O que funciona nesta fase

- Sobe 1..N backends MCP **stdio** (processos filhos), **HTTP** (JSON-RPC via
  POST) e **SSE** (stream de Server-Sent Events + POST na URL literal anunciada
  pelo evento `endpoint`) a partir de `config/config.json`, escolhendo o
  transporte pelo campo `type`.
- O `HttpClient` envia `Accept: application/json, text/event-stream` em todo
  POST e aceita as duas respostas previstas pelo transporte oficial Streamable
  HTTP: JSON direto ou `text/event-stream` com a resposta JSON-RPC
  correspondente. A compatibilidade foi validada com testes de protocolo para
  `initialize`, `tools/list` e `tools/call`; a validação manual contra um
  servidor MCP HTTP real de terceiro depende de esse servidor estar disponível.
  Robustez do transporte (clients http/sse/stdio): ids JSON-RPC únicos por
  request com correlação request→resposta validada em ambos os caminhos do
  `HttpClient` (JSON direto e SSE); envelope `jsonrpc: "2.0"` da resposta
  validado; Content-Type de resposta fora de JSON/SSE vira erro de transporte
  imediato (com o tipo recebido na mensagem); POST respondido com 4xx/5xx
  falha na hora (sem esperar o timeout) no `SseClient` e é registrado em log
  nas notificações (http e sse); o GET do stream SSE envia
  `Accept: text/event-stream` (o POST segue com JSON); headers obrigatórios do
  transporte Streamable HTTP vencem Content-Type/Accept custom do config
  (decisão deliberada, documentada no código); `StdioClient` faz cleanup de
  processo/tasks se o handshake falhar (nenhum órfão) e um erro inesperado no
  leitor de stdout invalida o client (`is_alive() → False`) para o Health
  Monitor detectar e reiniciar; `_fail_pending` e `_capabilities` vivem na
  `BaseClient` (sem duplicação e sem atributo mutável de classe).
- Handshake `initialize` com cada backend e registro de capabilities. A
  resposta do backend é validada (`protocolVersion` suportado + `capabilities`
  objeto) antes de o backend ser considerado pronto; respostas de listagem
  malformadas viram erro de domínio (não lista vazia silenciosa).
- **`BackendManager`** dono do ciclo de vida: um backend que falha ao subir no
  startup não derruba os demais (o Gateway só não sobe se TODOS falharem).
- **Health Monitor** (`health_check_interval_seconds`, default 5s): verifica
  cada backend (transporte vivo + `ping` com timeout curto) e loga detecção de
  queda, restart e recuperação — para stdio, http e sse.
- **Auto-restart com backoff exponencial** (1s → 2s → 4s → ... cap 30s):
  após `max_restart_attempts` tentativas consecutivas, o backend é marcado
  como `failed` (terminal, exige intervenção humana) e o monitor para de
  tentar. O restart roda em task própria por backend — não bloqueia o monitor
  nem os demais backends. Funciona para os três transportes: no stdio o
  processo filho é recriado; em http/sse o Gateway **reconecta** na `url`
  quando o servidor remoto volta (o Gateway não é pai desses processos).
- **Graceful shutdown** (Ctrl+C / SIGTERM / SIGBREAK no Windows): encerra em
  poucos segundos e garante que nenhum processo de backend fica órfão
  (validado por script que inspeciona os processos antes/depois). O cancelamento
  da task do health monitor é tratado como desfecho esperado (nunca vira
  traceback), o `Ctrl+C` é silenciado no entrypoint após o cleanup e o
  encerramento emite o log `gateway_shutdown_complete` (ou
  `gateway_shutdown_interrupted`, se o shutdown foi interrompido no meio).
- **`GET /health`** (sem auth, para checagens de infra) e **`GET /api/servers`**
  (mesma auth do `/mcp`) refletindo o estado real de cada backend.
- Registries agregados com snapshot imutável (swap atômico por referência) e
  namespace `backend.<id>`:
  - `tools/list` / `tools/call`
  - `resources/list` / `resources/read`
  - `prompts/list` / `prompts/get`
- Backend que não implementa resources/prompts é tratado como lista vazia
  (resposta `MethodNotFound` do backend vira ausência, não erro).
- Endpoint único `POST /mcp` (FastAPI) com autenticação **Bearer opcional**.
- Logging estruturado (`structlog`) com `request_id` por request via
  `contextvars`.
- Erros JSON-RPC padronizados (tabela abaixo) — nunca um 500 genérico.
- `initialize` e `ping` respondidos pelo próprio Gateway (necessários para
  clientes MCP reais se conectarem).

## Requisitos

- Python 3.10+
- Dependências em `requirements.txt`

## Instalação

> **Nota:** o config de exemplo (`config/config.json`) já está versionado com
> `auth_token: null` — clone e rode. Se você definir um token, não commite o
> arquivo (use `config/config.local.json`, que está no `.gitignore`, ou
> mantenha o token fora do repositório).

```bash
python -m venv .venv
# Windows (PowerShell): .venv\Scripts\Activate.ps1
source .venv/Scripts/activate   # Windows (Git Bash)
source .venv/bin/activate       # Linux/macOS
pip install -r requirements.txt
```

## Configuração (`config/config.json`)

```json
{
  "backends": [
    {
      "name": "backend-a",
      "command": "python",
      "args": ["tests/fake_backend.py"]
    },
    {
      "name": "remoto",
      "type": "http",
      "url": "http://127.0.0.1:9000",
      "headers": {"Authorization": "Bearer token-do-backend"},
      "request_timeout_seconds": 10
    },
    {
      "name": "eventos",
      "type": "sse",
      "url": "http://127.0.0.1:9001"
    }
  ],
  "auth_token": null,
  "max_payload_bytes": 10485760,
  "health_check_interval_seconds": 5,
  "auto_restart": true,
  "max_restart_attempts": 5,
  "backend_request_timeout_seconds": 30
}
```

Campos:

- `backends` — lista obrigatória. Cada backend tem `name` (único, vira prefixo
  de namespace; **sem pontos nem espaços** — o namespace é delimitado por `.`)
  e o restante depende do `type`:
  - `type: "stdio"` (default; configs antigos sem o campo continuam válidos):
    `command` obrigatório + `args`;
  - `type: "http"`: `url` obrigatória (JSON-RPC via POST); `command`/`args`
    proibidos;
  - `type: "sse"`: `url` obrigatória (POST `/messages` + stream de eventos);
    `command`/`args` proibidos;
  - comum a http/sse: `headers` (dict enviado em cada request — ex.: auth do
    próprio backend remoto) e `request_timeout_seconds`.
  Combinações inválidas (ex.: `type: "http"` com `command`, ou sem `url`)
  rejeitam o config com erro claro de validação.
- `auth_token` — opcional. Se definido, o `POST /mcp` exige o token via header
  `Authorization: Bearer <token>` **ou** via query string `?token=<token>`
  (ver [Autenticação](#autenticação)). Se `null`/omitido, o Gateway roda sem
  autenticação (adequado para uso local) e loga um aviso no startup.
- `max_payload_bytes` — opcional. Limite do corpo do `POST /mcp` (default
  10 MiB); acima disso o Gateway responde `413`.
- `health_check_interval_seconds` — opcional (default `5`). Intervalo entre
  ciclos do Health Monitor.
- `auto_restart` — opcional (default `true`). Se `false`, backends detectados
  como offline permanecem offline até intervenção manual (reiniciar o
  Gateway).
- `max_restart_attempts` — opcional (default `5`). Tentativas consecutivas de
  restart antes de marcar o backend como `failed` (estado terminal; os ciclos
  seguintes não tentam mais nada).
- `backend_request_timeout_seconds` — opcional (default `30`). Timeout global
  de cada request a um backend; pode ser sobreposto por backend em
  `request_timeout_seconds`.
- `session_ttl_seconds` — opcional (default `3600`). Tempo de vida (s) de uma
  sessão do **filtro seletivo** sem atividade (Fase 5); cada request com o
  mesmo `Mcp-Session-Id` renova o TTL, e sessão expirada volta a ver todos os
  backends.

O exemplo usa o `tests/fake_backend.py` (backend MCP fake com tools `echo`/`add`,
resources `memory://greeting` e `file:///tmp/fake-note.txt`, e o prompt `greet`).
Para usar um backend MCP real stdio, troque `command`/`args`:

```json
{
  "name": "filesystem",
  "command": "npx",
  "args": ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
}
```

### O transporte SSE em uma nota

No SSE o caminho de ida e o de volta são conexões diferentes: o Gateway envia
cada request JSON-RPC por um **POST no endpoint anunciado pelo servidor** e
recebe as respostas por um **stream GET separado** (`text/event-stream`) lido
continuamente em background.

O destino dos POSTs segue o padrão do spec HTTP+SSE do MCP: o **primeiro
evento** do stream é nomeado `endpoint` e seu `data` é a URL (relativa ou
absoluta, geralmente com um `session_id` próprio da conexão) que o cliente
DEVE usar para todos os POSTs daquela conexão — o Gateway usa a URL exatamente
como anunciada e a **recaptura do zero a cada (re)conexão** (a da conexão
anterior pode ter expirado). Servidores que não anunciam o `endpoint` (entre
eles o fake legado da Fase 3, via `--no-endpoint`) caem por fallback para a
rota fixa `/messages`, com o evento logado em debug (`sse_endpoint_fallback`).
Servidor que abre o stream e fica mudo (nenhum evento dentro do timeout de
10s) falha o start com erro claro — não trava.

A correlação request→resposta é feita pelo **`id` JSON-RPC** do payload (o
`id:` do protocolo SSE é outro campo e é ignorado). Se o stream cai no meio de
uma request, os requests pendentes falham imediatamente com
`-32002`/desconexão — nunca ficam pendurados até o timeout. Notificações
(incluindo `notifications/initialized` do handshake) também vão por POST,
sem resposta esperada.

> Correção pós-Fase 5: antes o Gateway POSTava sempre em `/messages` fixo,
> o que funcionava contra o fake mas dava **404** em servidores MCP-SSE reais
> (ex.: `mcp-proxy` do VSCode), que amarram o POST à URL anunciada com
> `session_id`. Essa limitação está resolvida.

## Rodando

```bash
python main.py
```

Variáveis de ambiente:

- `MCP_GATEWAY_PORT` — porta do Gateway (default `8080`).
- `MCP_GATEWAY_HOST` — endereço de bind do Gateway (default seguro `127.0.0.1`);
  defina `0.0.0.0` somente quando a exposição na rede for intencional.
- `MCP_GATEWAY_CONFIG` — caminho do config (default `config/config.json`).

No startup, os backends são iniciados e o log estruturado mostra quantas
tools/resources/prompts cada um registrou e o total agregado. Se `auth_token`
não estiver configurado, um aviso é logado.

## Autenticação

Com `auth_token` no config, o token é aceito de **duas formas**:

**1. Header (forma primária/recomendada):**

```bash
curl -s -X POST http://127.0.0.1:8080/mcp \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer SEU_TOKEN' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

**2. Query string `?token=` (alternativa para o Windows):**

```bash
curl -s -X POST "http://127.0.0.1:8080/mcp?token=SEU_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

Em ambos os casos: token ausente ou errado → `401` com
`WWW-Authenticate: Bearer`.

### Por que a query string existe no `POST /mcp`

No Windows, o Claude Desktop monta a linha de comando do servidor MCP via
`cmd.exe`, e a forma como ele escapa argumentos corrompe valores **com
espaço**: um `--header "Authorization: Bearer <token>"` passado ao `npx`
(mcp-remote) quebra a chamada inteira e o cliente termina com
`Server disconnected`. O token via query string embutido na URL resolve isso,
porque a URL não tem espaços:

```json
{
  "mcpServers": {
    "mcp-gateway": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "http://localhost:8080/mcp?token=SEU_TOKEN",
        "--allow-http"
      ]
    }
  }
}
```

Sem `--header` — a autenticação vai embutida na URL, imune ao problema de
escaping do `cmd.exe`.

### Precedência e segurança

- O **header Bearer é checado primeiro** e continua funcionando exatamente
  como antes (a query string é uma alternativa **adicional**, não uma
  substituição — nada muda para quem já usa `--header`).
- Se **ambas** as formas forem enviadas e **apenas uma** bater com o token
  correto, o acesso é **aceito**: basta uma forma correta, pois ela já prova
  conhecimento do token — exigir as duas não adicionaria segurança, só
  fragilidade. Essa regra vale para o `POST /mcp` e para o dashboard `GET /`.
- O dashboard também aceita `/?token=` (conveniência para navegador, mesma
  regra acima).
- As rotas `GET /api/servers`, `POST /api/servers/{name}/disable|enable|restart`
  e `GET /api/tools/size` aceitam **apenas** o header Bearer.

> **Nota de segurança:** tokens em query string podem ficar registrados em
> logs de acesso de proxies/servidores web intermediários, histórico de
> navegador (no caso do dashboard) e histórico de comandos do terminal. Para
> o Gateway rodando **local** isso é aceitável — e é o caso de uso pensado.
> Se for expor o Gateway além da máquina local, prefira o header Bearer e um
> canal cifrado (TLS/reverse proxy); o access log do próprio Gateway é
> desligado (`access_log=False` no uvicorn) e nenhum log estruturado inclui a
> URL da request, então o token não vaza nos logs do Gateway em si.

## Rotas de observabilidade

- `GET /health` — status geral (`ok`/`degraded`) e contagem de backends por
  estado, além de totals agregados de tools/resources/prompts.
  **Nunca exige autenticação** (mesmo com `auth_token` configurado): é o
  endpoint para load balancers/monitores de uptime, que não têm como declarar
  o Bearer; expõe apenas agregados, sem comando/args.
- `GET /api/servers` — detalhe operacional de cada backend (`type`, `command`,
  `args`, `url`, `status`, `consecutive_failures`, `last_restart_at`,
  contagens por backend, transporte). **Respeita a mesma auth do `POST /mcp`**
  (`401` sem o Bearer quando `auth_token` está configurado) porque expõe
  detalhes operacionais que não devem ficar públicos.

```bash
# /health: aberto mesmo com auth configurada
curl -s http://127.0.0.1:8080/health

# /api/servers: precisa do Bearer (se auth configurada)
curl -s http://127.0.0.1:8080/api/servers -H 'Authorization: Bearer SEU_TOKEN'

# Rotas de controle (Fase 4): mesma auth do /api/servers
# POST /api/servers/{name}/disable | enable | restart — ver seção própria
```

Estados de um backend: `running`, `offline` (detectado como caído),
`restarting` (aguardando backoff/reiniciando), `failed` (terminal — esgotou
`max_restart_attempts`; recupere com `POST /api/servers/{name}/restart` ou
reiniciando o Gateway) e `disabled` (desligado **intencionalmente** via API —
estado neutro, NÃO é falha; o Health Monitor não tenta reiniciá-lo e o
`/health` não degrada por causa dele; a única saída é o `enable`).

## Testando queda/restart de um backend HTTP/SSE

O fluxo da seção anterior (matar o processo stdio) funciona igual; para
backends remotos a diferença é o significado do restart:

1. Derrube o servidor remoto (ex.: `Stop-Process` no PID que escuta a porta
   do backend http/sse — `netstat -ano | findstr <porta>` mostra o PID).
2. Log: `sse_stream_perdido` (imediato, para SSE — emitido sempre que um
   stream já estabelecido termina por falha, inclusive se um `stop()`
   corre em paralelo) e `backend_detected_offline` no próximo ciclo;
   `/health` vira `degraded` e as tools do backend saem do registry
   (chamadas retornam `-32001` até a recuperação). Para HTTP não há evento
   de queda síncrono — a detecção vem do ciclo do monitor.
3. Restart tentado com o servidor fora loga `backend_restart_failed`
   (ConnectError) com backoff crescente — sem derrubar o Gateway.
4. Suba o servidor de volta **na mesma porta** (a `url` do config não muda):
   a próxima tentativa loga `backend_recovered`, as tools voltam ao registry
   e as chamadas funcionam novamente.

Nota: para http/sse o Gateway não é pai do processo — o restart NÃO cria
processo nenhum, apenas reconecta quando o servidor remoto volta ao ar.

## Controle manual dos backends (Fase 4)

Três rotas comandam um backend pelo nome (o mesmo do config), todas com a
**mesma autenticação do `/mcp`** — são operações que alteram o estado do
Gateway, nunca abertas:

```bash
# Desliga intencionalmente: para o processo, remove as tools do tools/list
# e entra no estado 'disabled' (o Health Monitor NÃO tenta reiniciá-lo).
curl -s -X POST http://127.0.0.1:8080/api/servers/backend-a/disable \
  -H 'Authorization: Bearer SEU_TOKEN'

# Reverte o 'disabled': sobe o backend de novo e reintegra nos registries
curl -s -X POST http://127.0.0.1:8080/api/servers/backend-a/enable \
  -H 'Authorization: Bearer SEU_TOKEN'

# Restart manual imediato — funciona inclusive com o backend 'running'
# (restart de manutenção) ou 'failed' (única recuperação sem reiniciar o GW)
curl -s -X POST http://127.0.0.1:8080/api/servers/backend-a/restart \
  -H 'Authorization: Bearer SEU_TOKEN'
```

Cada resposta inclui o estado resultante:
`{"backend":"backend-a","action":"disable","status":"disabled"}`.

Códigos HTTP: `404` backend inexistente no config · `409` operação incompatível
com o estado atual (ex.: `enable` num backend que não está `disabled`) ·
`503` a subida falhou (detalhe no corpo; o Gateway continua de pé) · `401`
sem/ com token errado.

Um backend `disabled` aparece como `"disabled"` no `GET /health` e no
`GET /api/servers` (com contagens zeradas), **não** degrada o status geral e
loga `backend_disabled_ignorado_pelo_monitor` uma única vez se o monitor
passar por ele.

## Filtro seletivo de backends por sessão (Fase 5)

**Por quê**: o Gateway é pensado para ser consumido por um agente (OpenClaude)
com modelos gratuitos via OpenRouter — contexto limitado e tool-calling menos
confiável. Expor todas as tools de todos os backends de uma vez pode estourar
o contexto ou confundir o modelo. O filtro deixa a sessão ativar só o
subconjunto de backends de que precisa.

**Compatibilidade**: é uma **extensão do Gateway** (não faz parte do spec
MCP). Cliente que não a conhece (Claude Desktop, `mcp-remote`, qualquer
cliente MCP) funciona EXATAMENTE como antes — sem `Mcp-Session-Id` não há
sessão, e sem sessão não há filtro: `tools/list` retorna tudo, como sempre
retornou. Nenhum comportamento existente mudou (coberto por teste de
não-regressão).

### Protocolo

A sessão é identificada pelo header HTTP **`Mcp-Session-Id`** (qualquer string
escolhida pelo cliente; o mesmo valor em todas as chamadas daquela sessão).
Três métodos JSON-RPC customizados, via `POST /mcp`:

```bash
S="minha-sessao-01"
URL=http://127.0.0.1:8080/mcp
AUTH="Authorization: Bearer SEU_TOKEN"

# Ativa só um subconjunto de backends para esta sessão (extensão)
curl -s -X POST $URL -H 'Content-Type: application/json' -H "$AUTH" \
  -H "Mcp-Session-Id: $S" \
  -d '{"jsonrpc":"2.0","id":1,"method":"gateway/session/set_active_backends","params":{"backends":["backend-a"]}}'
# -> {"result":{"active_backends":["backend-a"]}}

# A partir daqui, esta sessão só enxerga backend-a:
curl -s -X POST $URL -H 'Content-Type: application/json' -H "$AUTH" \
  -H "Mcp-Session-Id: $S" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

# Consultar o filtro da sessão (null = sem filtro)
# gateway/session/get_active_backends  -> {"result":{"active_backends":[...],"filtered":true}}

# Remover o filtro (sessão volta a ver tudo)
# gateway/session/clear_active_backends
```

Semântica:
- com filtro ativo, `tools/list`, `resources/list` e `prompts/list` da sessão
  retornam só itens dos backends ativados;
- `tools/call`/`resources/read`/`prompts/get` de item **fora do filtro**
  respondem `-32001` `Unknown tool/resource/prompt: ...` — o MESMO erro de um
  item inexistente. Do ponto de vista da sessão filtrada, bloqueado é como se
  não existisse (o filtro não vaza a existência);
- `backends` com nomes desconhecidos → `-32602` com a lista dos backends
  válidos no `data` (falha explícita, nunca filtro parcial silencioso);
- chamar os métodos de sessão sem o header → `-32600`.

O filtro é uma **view por sessão**: os registries globais continuam sendo a
fonte única de verdade (o que outros clientes e o dashboard veem não muda), e
reinícios de backend continuam refletindo normalmente na view filtrada.

### TTL de sessão

Sessão sem atividade expira após `session_ttl_seconds` (default 3600s) e volta
a ver todos os backends — comportamento seguro, nunca o contrário (expiração
nunca bloqueia). Qualquer request com o mesmo `Mcp-Session-Id` renova o TTL;
limpeza de sessões abandonadas é oportunista (por contagem/intervalo), sem
acumular memória.

### Medindo se vale a pena: `GET /api/tools/size`

Diagnóstico simples (mesma auth do `/api/servers`): tamanho do `tools/list` —
contagem de tools, caracteres do JSON (`len(json.dumps)`), tokens aproximados
(chars ÷ 4, heurística grosseira, **não** é tokenizer real) e quebra por
backend. Com `Mcp-Session-Id`, mede o tools/list DAQUELA sessão — mostra na
prática quanto o filtro economiza:

```bash
curl -s http://127.0.0.1:8080/api/tools/size -H "$AUTH"
curl -s http://127.0.0.1:8080/api/tools/size -H "$AUTH" -H "Mcp-Session-Id: $S"
```

No config de exemplo (3 backends × 2 tools): 6 tools ≈ 1 447 chars ≈ 361
tokens; filtrando 2 de 3 backends, 4 tools ≈ 969 chars ≈ 242 tokens (**34%
menor**) — e a economia cresce linearmente com o número de backends/tools.

### Roteiro manual com o agente real (OpenClaude + OpenRouter)

A validação com o agente depende da infra do usuário (chave OpenRouter, app do
OpenClaude), então fica como roteiro manual — o terreno está preparado:

1. Meça o ponto de partida: `GET /api/tools/size` (sem sessão). Anote
   `approx_tokens` — é o peso do catálogo que entra no contexto a cada turno de
   tool-calling.
2. Configure o OpenClaude apontando para o Gateway (via `mcp-remote --allow-http
   http://127.0.0.1:8080/mcp`, como nas fases anteriores) com um modelo
   gratuito do OpenRouter.
3. Antes de dar a tarefa ao agente, ative o filtro para a sessão que o cliente
   usa (mesmo `Mcp-Session-Id` em todas as chamadas):
   `gateway/session/set_active_backends` com só o(s) backend(s) relevantes à
   tarefa. Confirme o ganho com `GET /api/tools/size` na mesma sessão.
4. Compare o comportamento do modelo com e sem filtro: com menos tools no
   catálogo, modelos menores erram menos na escolha/argumentos de tool (menos
   nomes parecidos para confundir, schema menor no contexto) e sobra contexto
   útil para a conversa.
5. Repita uma tarefa real de ferramenta (ex.: `backend-a.echo`) e confirme que
   o fluxo de chamada funciona identicamente dentro da sessão filtrada.

## Dashboard read-only (Fase 4)

`GET /` serve uma página HTML server-rendered (sem JavaScript) com: status
geral do Gateway, contadores agregados e uma tabela dos backends (nome, tipo,
status, tools/resources/prompts, falhas consecutivas, url ou comando).

**Autenticação: a mesma do `/api/servers`.** O dashboard expõe detalhes
operacionais (comandos, urls) e deve ser protegido tanto quanto elas. Como
navegador não declara header Bearer, o Gateway aceita `/?token=SEU_TOKEN`
quando `auth_token` está configurado (com `auth_token` nulo, o dashboard fica
aberto, igual ao resto da API). Valem as regras de
[precedência da seção Autenticação](#precedência-e-segurança): basta uma
das formas (header ou `?token=`) estar correta.

```bash
curl -s "http://127.0.0.1:8080/?token=SEU_TOKEN" | head -20
```

## Importador de claude_desktop_config.json (Fase 4)

Converte a seção `mcpServers` do config do Claude Desktop para o formato do
Gateway:

```bash
# Default do Windows: %APPDATA%\Claude\claude_desktop_config.json
python scripts/import_claude_desktop_config.py

# Caminho explícito, gravando em config/config.imported.json (default)
python scripts/import_claude_desktop_config.py caminho/para/claude_desktop_config.json

# Só olhar (não grava nada; JSON puro no stdout, avisos no stderr)
python scripts/import_claude_desktop_config.py --stdout

# Revisar antes: lista os servidores encontrados
python scripts/import_claude_desktop_config.py --list-servers

# Evita colisão de nomes ao mesclar num config que já tem backends
python scripts/import_claude_desktop_config.py --prefix claude
```

Comportamento:
- entradas `command`/`args` viram backends `type: "stdio"`;
- entradas já remotas (`url` + `type: http|sse`) são convertidas diretamente;
- nomes são sanitizados para o padrão do Gateway (espaços/pontos viram `-`,
  com aviso — o nome vira prefixo de namespace);
- o campo `env` do Claude **não é aplicado** (o Gateway não suporta env por
  backend) — a entrada é convertida e o campo sinalizado em aviso;
- variáveis `%ENV%`/`$ENV` em command/args são expandidas com os valores
  atuais do ambiente (best-effort).

**Limitações conhecidas — o que NÃO é convertido automaticamente** (sempre
sinalizado em aviso, nunca convertido errado em silêncio):
- **pontes** (`mcp-remote`, `supergateway`, `mcp-proxy`, e qualquer comando
  cujo nome contenha remote/proxy/gateway/bridge): declaração manual —
  `"type": "http"`/`"sse"` + `url` no config do Gateway;
- entradas com `url` sem `type` (o Gateway exige o transporte declarado);
- entradas sem `command` nem `url` utilizáveis;
- campos desconhecidos (`cwd`, `shell`, etc.) são ignorados com aviso.

**Segurança de gravação**: o importador NUNCA sobrescreve silenciosamente.
Sem flags grava em `config/config.imported.json` (arquivo separado para
revisão e mesclagem manual — o Gateway não tem hot-reload); destino já
existente exige `--force` explícito; `--stdout` apenas imprime o JSON
(avisos vão para o stderr, então a saída continua sendo JSON puro). Se nada
for convertido, o comando falha com erro em vez de gerar um config vazio.

## Testando queda/restart de um backend manualmente

1. Rode o Gateway (`python main.py`) e anote os PIDs dos processos de backend
   (o fake backend loga o próprio PID no stderr do Gateway:
   `fake-backend pid=NNNN`).
2. Mate um backend (PowerShell: `Stop-Process -Id NNNN -Force`).
3. Observe no log, no próximo ciclo do monitor: `backend_detected_offline` →
   `backend_restart_scheduled` (com o `delay_seconds` do backoff) →
   `backend_recovered` com novo PID, e as tools re-registradas sem duplicatas
   (`GET /health` passa de `degraded` de volta para `ok`).
4. `GET /api/servers` mostra `consecutive_failures` e `last_restart_at` do
   backend recuperado. Durante o tempo offline, chamadas às tools daquele
   backend respondem erro `-32002` (backend indisponível) sem derrubar o
   Gateway.
5. Ctrl+C no Gateway: encerramento rápido e nenhum backend órfão (confira com
   `Get-Process python` antes/depois). O log termina com
   `health_monitor_stopped` → `backend_manager_stopped` →
   `gateway_shutdown_complete`, sem nenhum traceback na saída.

## Logging estruturado

Todo log dos módulos do Gateway passa pelo `structlog`. Cada request HTTP ganha
um `request_id` (UUID) vinculado via `contextvars` — qualquer log emitido
durante o processamento daquela request (incluindo dentro do `McpServer` e dos
clients) carrega o mesmo `request_id` automaticamente, sem passá-lo de função
em função. Os logs incluem: request recebida (method, id), backend escolhido
para roteamento, sucesso/erro e tempo de resposta (`duration_ms`).

Exceções de requests abandonadas (ex.: um health check que desiste do `ping`
pouco antes da resposta chegar) são sempre consumidas pelas futures e
registradas em debug (`future_exception_descartada`) — nada vaza como aviso
do asyncio ("Future exception was never retrieved") fora do structlog.

## Namespacing de URIs de resources

Tools e prompts são expostos como `backend.nome`. Resources são endereçados por
URI, então o mesmo prefixo é aplicado à string inteira: a URI original
`file:///tmp/a.txt` do backend `fs` vira `fs.file:///tmp/a.txt` no
`resources/list` — o resultado continua sendo uma URI válida (o prefixo vira
parte do scheme), é reversível e não depende do formato do resource. O
`resources/read` aceita a URI namespaced, chama o backend com a URI original e
reescreve a `uri` do conteúdo devolvido de volta para a forma namespaced
(mantendo o round-trip para o cliente). Decisão documentada no
`gateway/registries/resource_registry.py`.

## Testando com curl

```bash
# Tools agregadas (namespaced)
curl -s -X POST http://127.0.0.1:8080/mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

# Chama uma tool namespaced (rota para o backend certo)
curl -s -X POST http://127.0.0.1:8080/mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"backend-a.echo","arguments":{"text":"oi"}}}'

# Resources agregados (URI namespaced)
curl -s -X POST http://127.0.0.1:8080/mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":3,"method":"resources/list"}'

# Lê um resource pela URI namespaced
curl -s -X POST http://127.0.0.1:8080/mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":4,"method":"resources/read","params":{"uri":"backend-a.memory://greeting"}}'

# Prompts agregados e um prompt específico
curl -s -X POST http://127.0.0.1:8080/mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":5,"method":"prompts/list"}'
curl -s -X POST http://127.0.0.1:8080/mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":6,"method":"prompts/get","params":{"name":"backend-a.greet","arguments":{"person":"Ana"}}}'
```

## Códigos de erro JSON-RPC

| Caso | Código | Onde |
| --- | --- | --- |
| JSON malformado | `-32700` ParseError | HTTP `400` |
| Envelope inválido / campo obrigatório ausente (`jsonrpc`/`method`) ou `id` inválido | `-32600` InvalidRequest | `200` com corpo de erro |
| Method desconhecido | `-32601` MethodNotFound | `200` com corpo de erro |
| Parâmetro inválido de método conhecido (ex.: `tools/call` sem `name`, `initialize` incompatível ou `params` não-objeto) | `-32602` InvalidParams | `200` com corpo de erro |
| Tool/resource/prompt namespaced inexistente | `-32001` (custom) | `200` com corpo de erro |
| Backend indisponível/processo morto durante a chamada | `-32002` (custom) | `200` com corpo de erro |
| Backend remoto fora do ar / stream SSE caído | `-32002` (custom) | `200` com corpo de erro |
| Erros JSON-RPC vindos do backend | código original do backend | repassado |

Validações de transporte (Content-Type diferente de `application/json` →
`415`; parâmetros opcionais como `charset` são aceitos; payload acima do
limite → `413`; auth falha → `401`) respondem com HTTP status próprio e corpo
JSON descritivo — nunca um 500 genérico. O limite é verificado primeiro pelo
`Content-Length`, quando disponível, e também durante o streaming de corpos
chunked, antes de acumular dados além do limite.

Nota: ausência de campo no *envelope* retorna `InvalidRequest` (-32600), não
`InvalidParams` (-32602) — pelo spec do JSON-RPC 2.0, um Request sem
`method`/`jsonrpc` é um Request inválido; `InvalidParams` fica reservado para
parâmetros inválidos de um método conhecido.

O Gateway negocia `protocolVersion` durante `initialize` usando o conjunto de
versões MCP suportadas (atualmente `2024-11-05`). O payload precisa conter
`protocolVersion` (string), `capabilities` (objeto) e `clientInfo` com
`name`/`version` (strings). Entradas de tools, resources ou prompts com
metadata estruturalmente inválida são omitidas da listagem e registradas em
log de aviso, sem derrubar o Gateway.

## Testes

```bash
pytest
```

Os testes usam backends MCP fake — nunca MCPs reais de terceiros:
`tests/fake_backend.py` (stdio, subprocesso real), `tests/fake_backend_http.py`
(JSON-RPC via POST, subprocesso real) e `tests/fake_backend_sse.py`
(POST `/messages` + stream SSE, subprocesso real) — os três com a mesma
semântica de protocolo (`tests/fake_logic.py`) e flags `--no-resources`/
`--no-prompts` para exercitar o tratamento gracioso de backends que não
implementam esses métodos. A suíte cobre: registries
(registro/sobrescrita/remoção/colisão), handlers de tools/resources/prompts,
validações HTTP (Content-Type, payload grande, JSON malformado, auth 401/200),
consistência do `request_id` nos logs, ciclo de vida do `BackendManager`,
detecção de queda, restart com backoff (tempos zerados nos testes), limite de
tentativas (`failed` terminal), rotas `/health`/`/api/servers` (política de
auth), shutdown sem processos órfãos e regressão do Ctrl+C, validação do
config por transporte, `HttpClient` e `SseClient` contra
subprocessos reais (handshake, listagens, chamadas, headers do config,
timeout, backend fora do ar, erro JSON-RPC via stream), queda do stream SSE
falhando pendentes imediatamente, integração com os três tipos simultâneos
(agregação + roteamento) e health/auto-restart reconectando backends remotos —
e, da Fase 4: `disable`/`enable`/`restart` no manager e via HTTP (404/409/503,
auth), monitor não tocando backends `disabled`, restart manual recuperando
`failed`, cancelamento de restart pendente, dashboard HTML (auth + `?token=`)
e o importador (conversão, sanitização de nomes, pontes não convertidas com
aviso, política de gravação `--stdout`/`--output`/`--force`) — e, da Fase 5:
não-regressão sem sessão/filtro, protocolo `gateway/session/*` (set/get/clear,
validação com `-32602`, sem header `-32600`), filtragem de
`tools/resources/prompts/list` por sessão, isolamento entre sessões
simultâneas, item bloqueado respondendo o MESMO erro de inexistente (`-32001`),
expiração por TTL com clock fake e o endpoint de diagnóstico `/api/tools/size`.

## Escopo e pendências

- O transporte é JSON-RPC puro sobre `POST /mcp` (stateless). A dança de
  sessão do streamable HTTP (`Mcp-Session-Id`, 202) e o suporte a batch ficam
  para fases futuras.
- Restart de backend stdio usa a mesma `command`/`args` do config (sem
  hot-reload de config — decisão do ROADMAP: reiniciar o processo é aceitável
  na v1). Para http/sse o restart reconecta na `url` declarada.
- O fake SSE segue o spec HTTP+SSE: emite o evento `endpoint` como primeiro
  evento (com `session_id` validado — POST fora da URL anunciada vira 404,
  como servidores reais) e tem flags para os caminhos alternativos do
  `SseClient`: `--no-endpoint` (modo legado, fallback `/messages`),
  `--silent` (stream mudo, exercita o timeout do start) e `--keepalive N`
  (intervalo dos keep-alives). Limitação conhecida (suficiente para os
  testes, que usam um cliente por fake): as respostas são espalhadas para
  todas as conexões abertas — sessões multi-cliente com session id ficam
  para fases futuras.
- O `SseClient` usa literalmente o `data` do evento `endpoint`: URLs
  absolutas não são alteradas; URLs relativas são resolvidas com `urljoin`
  contra a URL do stream (inclusive paths iniciados por `/`, que substituem o
  path do stream). Cada captura gera o log estruturado
  `sse_endpoint_capturado`, por exemplo:
  `backend=sse-test endpoint_raw=/message?sessionId=x
  endpoint=http://127.0.0.1:6010/message?sessionId=x`.
  A integração com servidores SSE reais de terceiros foi validada contra esse
  contrato, incluindo o cenário de rota fora de `/sse`.
- O dashboard (`GET /`) é server-rendered e read-only: sem JavaScript e sem
  ações na página — controle é via `/api/*` (decisão do ROADMAP: "dashboard
  começa read-only; ações via `/api/*` diretamente").
- **Métricas (Prometheus/OpenTelemetry): adiadas**, conforme a própria condição
  do ROADMAP ("se o número de backends justificar"). O projeto é de uso
  pessoal com poucos backends — `/health`, `/api/servers` e os logs
  estruturados com `request_id` cobrem a observabilidade necessária hoje, e
  antecipar a dependência seria peso de manutenção sem uso (regra do
  `AGENT_INSTRUCTIONS`: não introduzir dependência sem necessidade clara).
  O diagnóstico prático desta fase é o `GET /api/tools/size`.
- O filtro seletivo é extensão do Gateway (`gateway/session/*`) — o método
  customizado NÃO integra o spec MCP; clientes padrão nunca precisam dele.
- Pendente para a **Fase 6** do `ROADMAP.md`: MCPs customizados próprios e
  adapters de auto-instalação em outros apps (Cursor, VSCode, etc.) — fora do
  escopo do McpSentinel, conforme o ROADMAP, projeto separado.

## Licença

[Distribuído sob a licença MIT](LICENSE).
