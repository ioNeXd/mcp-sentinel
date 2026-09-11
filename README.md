# McpSentinel (MCP Gateway)  
  
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)  
  
Um único ponto de entrada para todos os seus servidores MCP. O McpSentinel  
inicia, agrega e monitora vários backends MCP (locais e remotos) e os expõe  
como se fossem um só — com um dashboard web para operar tudo pelo navegador.  
  
> **Status:** Fase 7 concluída. Dashboard interativo, controle de backends,  
> filtro por sessão e importador de configs. Rode `pytest` para validar a suíte.  
  
---  
  
## Sumário  
  
- [O que é](#o-que-é)  
- [Início rápido](#início-rápido)  
- [Dashboard (forma principal de uso)](#dashboard-forma-principal-de-uso)  
- [Configuração](#configuração)  
- [Autenticação](#autenticação)  
- [Conectando um cliente MCP](#conectando-um-cliente-mcp)  
- [Importando do Claude Desktop](#importando-do-claude-desktop)  
- [API HTTP](#api-http)  
- [Filtro seletivo por sessão](#filtro-seletivo-por-sessão)  
- [Referência técnica](#referência-técnica)  
  - [Transportes](#transportes)  
  - [Ciclo de vida e auto-restart](#ciclo-de-vida-e-auto-restart)  
  - [Namespacing](#namespacing)  
  - [Códigos de erro JSON-RPC](#códigos-de-erro-json-rpc)  
  - [Logging estruturado](#logging-estruturado)  
- [Testes](#testes)  
- [Escopo e pendências](#escopo-e-pendências)  
- [Licença](#licença)  
  
---  
  
## O que é  
  
Clientes MCP (Claude Desktop, Cursor, agentes) normalmente falam com **um**  
servidor por vez. O McpSentinel fica no meio: você lista seus backends num  
arquivo de configuração, o Gateway sobe todos, e o cliente enxerga **um único  
endpoint** (`POST /mcp`) com todas as tools, resources e prompts agregados.  
  
O que ele faz:  
  
- Agrega backends **stdio** (processos locais), **HTTP** e **SSE** (remotos) num  
  só endpoint, com namespacing automático (`backend.tool`).  
- Monitora a saúde de cada backend e **reinicia automaticamente** os que caem.  
- Oferece um **dashboard web** para ver o estado, controlar e adicionar backends.  
- Suporta autenticação por token, logging estruturado e desligamento gracioso.  
  
O Gateway **não** descobre servidores sozinho: você declara cada backend no  
`config/config.json` (ou adiciona pelo dashboard).  
  
---  
  
## Início rápido  
  
Requisitos: **Python 3.10+**.  
  
```bash  
python -m venv .venv  
source .venv/bin/activate        # Linux/macOS  
# .venv\Scripts\Activate.ps1     # Windows (PowerShell)  
pip install -r requirements.txt  
  
python main.py  
```  
  
Ao subir, o Gateway abre o **dashboard** no navegador automaticamente  
(`http://127.0.0.1:8080/`). O config de exemplo (`config/config.json`) já vem  
pronto com um backend fake para você testar de imediato.  
  
Variáveis de ambiente (opcionais):  
  
| Variável | Default | Descrição |  
| --- | --- | --- |  
| `MCP_GATEWAY_PORT` | `8080` | Porta do Gateway. |  
| `MCP_GATEWAY_HOST` | `127.0.0.1` | Bind. Use `0.0.0.0` só se for expor na rede. |  
| `MCP_GATEWAY_CONFIG` | `config/config.json` | Caminho do arquivo de config. |  
| `MCP_GATEWAY_OPEN_BROWSER` | `true` | `false` desliga o auto-open (servidor/headless). |  
  
---  
  
## Dashboard (forma principal de uso)  
  
Acesse `http://127.0.0.1:8080/`. É a maneira recomendada de operar o Gateway  
no dia a dia — não precisa de linha de comando.  
  
O dashboard mostra, em tempo real:  
  
- **Status geral** do Gateway e contadores agregados (tools, resources, prompts).  
- **Cards por backend**, agrupados por estado (Rodando / Reiniciando-Offline /  
  Falhou / Desabilitado), com tipo, url ou comando, contagens e falhas  
  consecutivas.  
- **Console de logs ao vivo**, com filtro por nível e busca (via Server-Sent  
  Events — atualiza sozinho, sem recarregar a página).  
  
E permite **agir** direto na página:  
  
- **Restart / Disable / Enable** de cada backend, pelos botões do card.  
- **+ Adicionar MCP**: um formulário que grava o novo backend no `config.json`  
  e já tenta subi-lo na hora — sem reiniciar o Gateway.  
- **Exportar snapshot**: baixa um JSON com `/health`, `/api/servers` e o  
  diagnóstico de tamanho das tools.  
- **Somente leitura**: esconde os botões de ação (útil para deixar aberto sem  
  risco de clique acidental; a preferência persiste entre recarregamentos).  
  
O primeiro carregamento vem renderizado no servidor (sem tela em branco) e, a  
partir daí, o JavaScript embutido assume o refresh e as ações. Não há build  
step nem dependência de front-end.  
  
**Autenticação:** se houver `auth_token` no config, o dashboard exige o mesmo  
token. Como o navegador não envia header `Authorization`, acesse com  
`http://127.0.0.1:8080/?token=SEU_TOKEN` — ver [Autenticação](#autenticação).  
  
---  
  
## Configuração  
  
O Gateway lê `config/config.json` (ou o caminho em `MCP_GATEWAY_CONFIG`):  

## Config
  
O `config/config.json` versionado traz **apenas os backends de exemplo/  
compartilhados** (`remoto`, `eventos`, `backend-teste`) e `auth_token: null`  

```json  
{  
  "backends": [  
    {  
      "name": "remoto",  
      "type": "http",  
      "url": "http://127.0.0.1:9000"  
    },  
    {  
      "name": "eventos",  
      "type": "sse",  
      "url": "http://127.0.0.1:9001"  
    },  
    {  
      "name": "backend-teste",  
      "command": "python",  
      "args": [  
        "tests/fake_backend.py"  
      ]  
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
  
**Backends** (`backends`, obrigatório — pelo menos um). Cada entrada tem um  
`name` único (vira o prefixo de namespace; só letras, números, `-` e `_`) e  
campos que dependem do `type`:  
  
| `type` | Campos | Observação |  
| --- | --- | --- |  
| `stdio` (default) | `command` (obrigatório), `args` | Processo local. |  
| `http` | `url` (obrigatória) | JSON-RPC via POST. |  
| `sse` | `url` (obrigatória) | POST + stream de eventos. |  
  
Backends `http`/`sse` aceitam ainda `headers` (enviados em cada request, ex.:  
auth do próprio backend remoto) e `request_timeout_seconds`. Combinações  
inválidas (ex.: `http` com `command`) são rejeitadas no boot com erro claro.  
  
**Parâmetros globais** (todos opcionais):  
  
| Campo | Default | Descrição |  
| --- | --- | --- |  
| `auth_token` | `null` | Token Bearer. `null` = sem auth (uso local; loga aviso). |  
| `max_payload_bytes` | 10 MiB | Limite do corpo do `POST /mcp` (`413` acima). |  
| `health_check_interval_seconds` | `5` | Intervalo entre ciclos de health check. |  
| `auto_restart` | `true` | Reinicia backends que caem. |  
| `max_restart_attempts` | `5` | Tentativas antes de marcar `failed` (terminal). |  
| `backend_request_timeout_seconds` | `30` | Timeout por request (sobreponível por backend). |  
| `session_ttl_seconds` | `3600` | TTL do [filtro por sessão](#filtro-seletivo-por-sessão). |  
  
> **Nota:** o config de exemplo já vem com `auth_token: null` — clone e rode.  
> Se definir um token, não commite o arquivo (use `config/config.local.json`,  
> que está no `.gitignore`).  
  
Exemplo de backend real stdio:  
  
```json  
{ "name": "filesystem", "command": "npx",  
  "args": ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"] }  
```  
  
---  
  
## Autenticação  
  
Com `auth_token` no config, o token é aceito de duas formas:  
  
1. **Header (recomendado):** `Authorization: Bearer SEU_TOKEN`.  
2. **Query string:** `?token=SEU_TOKEN` na URL.  
  
```bash  
curl -s -X POST http://127.0.0.1:8080/mcp \  
  -H 'Content-Type: application/json' -H 'Authorization: Bearer SEU_TOKEN' \  
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'  
```  
  
Token ausente/errado → `401` com `WWW-Authenticate: Bearer`.  
  
**Por que existe a query string:** no Windows, o Claude Desktop monta a linha  
de comando via `cmd.exe`, que corrompe headers com espaço (o  
`--header "Authorization: Bearer ..."` do `mcp-remote` quebra a chamada). O  
token na URL não tem espaço e resolve o problema.  
  
**Regras de precedência:** o header é checado primeiro; a query string é uma  
alternativa adicional. Se ambos forem enviados e apenas um bater, o acesso é  
aceito (basta uma prova correta). O dashboard (`GET /`) e o console de logs  
(`GET /api/logs/stream`) seguem a mesma regra. As rotas de controle e  
`/api/servers` aceitam **apenas** o header Bearer.  
  
> **Segurança:** tokens em query string podem aparecer em logs de proxies e no  
> histórico de navegador/terminal. Para uso **local** é aceitável (o caso de  
> uso pensado). Para expor além da máquina, prefira o header + TLS. O Gateway  
> desliga o access log do uvicorn e nunca loga a URL da request, então o token  
> não vaza nos logs do próprio Gateway.  
  
---  
  
## Conectando um cliente MCP  
  
Aponte o cliente para o endpoint agregado via `mcp-remote`:  
  
```json  
{  
  "mcpServers": {  
    "mcp-gateway": {  
      "command": "npx",  
      "args": ["-y", "mcp-remote", "http://localhost:8080/mcp?token=SEU_TOKEN", "--allow-http"]  
    }  
  }  
}  
```  
  
Sem `--header`: a autenticação vai embutida na URL (imune ao escaping do  
Windows). A partir daí, o cliente vê todas as tools/resources/prompts de todos  
os backends, com nomes namespaced (`backend.tool`).  
  
Testando direto com `curl`:  
  
```bash  
# Lista tools agregadas (namespaced)  
curl -s -X POST http://127.0.0.1:8080/mcp -H 'Content-Type: application/json' \  
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'  
  
# Chama uma tool (roteada para o backend certo)  
curl -s -X POST http://127.0.0.1:8080/mcp -H 'Content-Type: application/json' \  
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"backend-a.echo","arguments":{"text":"oi"}}}'  
```  
  
`resources/list`, `resources/read`, `prompts/list` e `prompts/get` funcionam da  
mesma forma.  
  
---  
  
## Importando do Claude Desktop  
  
Converte a seção `mcpServers` do config do Claude Desktop para o formato do  
Gateway:  
  
```bash  
# Default do Windows: %APPDATA%\Claude\claude_desktop_config.json  
python scripts/import_claude_desktop_config.py  
  
python scripts/import_claude_desktop_config.py caminho/claude_desktop_config.json  
python scripts/import_claude_desktop_config.py --stdout        # só imprime, não grava  
python scripts/import_claude_desktop_config.py --list-servers  # lista o que achou  
python scripts/import_claude_desktop_config.py --prefix claude # evita colisão de nomes  
```  
  
- Entradas `command`/`args` viram `type: "stdio"`; entradas remotas  
  (`url` + `type`) são convertidas diretamente.  
- Nomes são sanitizados (espaços/pontos viram `-`, com aviso).  
- **Não convertido automaticamente** (sempre com aviso, nunca em silêncio):  
  pontes (`mcp-remote`, `supergateway`, `mcp-proxy`…), entradas com `url` sem  
  `type`, entradas sem `command`/`url`, e o campo `env` (não suportado).  
- **Gravação segura:** nunca sobrescreve em silêncio (grava em  
  `config/config.imported.json`; destino existente exige `--force`); a escrita  
  é atômica. Revise e mescle no seu config — não há hot-reload.  
  
---  
  
## API HTTP  
  
Além do `POST /mcp`, o Gateway expõe rotas de observabilidade e controle.  
  
**Observabilidade**  
  
- `GET /health` — status geral (`ok`/`degraded`) e contagem de backends por  
  estado. **Nunca exige auth** (para load balancers/monitores); expõe só  
  agregados.  
- `GET /api/servers` — detalhe operacional de cada backend (tipo, comando/url,  
  status, falhas, último restart). **Mesma auth do `POST /mcp`.**  
- `GET /api/tools/size` — diagnóstico do tamanho do `tools/list` (chars/tokens  
  aproximados, quebra por backend). Aceita `Mcp-Session-Id` para medir uma  
  sessão filtrada.  
  
**Controle** (mesma auth do `/mcp` — alteram estado, nunca abertas):  
  
```bash  
curl -X POST http://127.0.0.1:8080/api/servers/backend-a/disable -H 'Authorization: Bearer SEU_TOKEN'  
curl -X POST http://127.0.0.1:8080/api/servers/backend-a/enable  -H 'Authorization: Bearer SEU_TOKEN'  
curl -X POST http://127.0.0.1:8080/api/servers/backend-a/restart -H 'Authorization: Bearer SEU_TOKEN'  
```  
  
- `disable` para o backend e o tira do `tools/list` (o monitor não o reinicia).  
- `enable` reverte, subindo e reintegrando o backend.  
- `restart` funciona inclusive com o backend `running` (manutenção) ou  
  `failed` (única recuperação sem reiniciar o Gateway).  
  
Códigos: `404` inexistente · `409` estado incompatível · `503` a subida falhou  
· `401` sem token.  
  
**Estados de um backend:** `running`, `offline` (caiu), `restarting`  
(aguardando backoff), `failed` (esgotou as tentativas — terminal) e `disabled`  
(desligado de propósito; não é falha, não degrada o `/health`).  
  
---  
  
## Filtro seletivo por sessão  
  
Extensão do Gateway (**não** faz parte do spec MCP) pensada para agentes com  
modelos menores: expor todas as tools de todos os backends pode estourar o  
contexto ou confundir o modelo. O filtro deixa uma sessão ativar só o  
subconjunto de que precisa.  
  
A sessão é identificada pelo header `Mcp-Session-Id` (qualquer string escolhida  
pelo cliente). Clientes que não conhecem a extensão funcionam exatamente como  
antes: sem o header, não há filtro.  
  
```bash  
S="minha-sessao"; URL=http://127.0.0.1:8080/mcp; AUTH="Authorization: Bearer SEU_TOKEN"  
  
# Ativa só um subconjunto para esta sessão  
curl -s -X POST $URL -H 'Content-Type: application/json' -H "$AUTH" -H "Mcp-Session-Id: $S" \  
  -d '{"jsonrpc":"2.0","id":1,"method":"gateway/session/set_active_backends","params":{"backends":["backend-a"]}}'  
  
# A partir daqui, esta sessão só enxerga backend-a no tools/list  
```  
  
Métodos: `gateway/session/set_active_backends`, `get_active_backends`,  
`clear_active_backends`.  
  
Semântica: com filtro ativo, as listagens retornam só os backends ativados, e  
um item fora do filtro responde o **mesmo** erro de item inexistente (`-32001`,  
não vaza a existência). Nome de backend desconhecido → `-32602`. Método de  
sessão sem o header → `-32600`. A sessão expira após `session_ttl_seconds` sem  
atividade e volta a ver tudo (nunca bloqueia). O filtro é apenas uma view: os  
registries globais seguem como fonte única de verdade.  
  
---  
  
## Referência técnica  
  
### Transportes  
  
- **stdio** — processo filho; JSON-RPC delimitado por newline no stdin/stdout.  
- **HTTP** — JSON-RPC por POST. O Gateway envia  
  `Accept: application/json, text/event-stream` e aceita resposta em JSON ou em  
  `text/event-stream`, conforme o transporte Streamable HTTP oficial.  
- **SSE** — ida e volta são conexões distintas: cada request vai por POST no  
  endpoint anunciado pelo servidor (primeiro evento `endpoint` do stream, URL  
  usada literalmente e recapturada a cada reconexão), e as respostas chegam por  
  um stream GET separado. A correlação é pelo `id` JSON-RPC. Servidores sem o  
  evento `endpoint` caem em fallback para `/messages`.  
  
Em todos: ids únicos por request com correlação validada, envelope  
`jsonrpc: "2.0"` verificado, Content-Type inesperado vira erro de transporte  
imediato, e queda de conexão falha os pendentes na hora (sem esperar timeout).  
  
### Ciclo de vida e auto-restart  
  
O `BackendManager` é o dono do estado. Um backend que falha ao subir no startup  
não derruba os demais (o Gateway só não sobe se **todos** falharem). O  
**Health Monitor** verifica cada backend a cada ciclo (transporte vivo + `ping`  
curto), em paralelo com limite de concorrência; o primeiro ciclo roda logo no  
startup.  
  
Quando um backend cai, o **auto-restart** usa backoff exponencial (1s → 2s → 4s  
… cap 30s). Após `max_restart_attempts` falhas, vira `failed` (terminal). Para  
stdio, o processo é recriado; para http/sse, o Gateway **reconecta** na `url`  
quando o servidor remoto volta (não é pai desses processos).  
  
O registro nos três registries é **atômico** e usa snapshot imutável (swap por  
referência, sem locks para leitura): se uma parte falha, o que já foi feito é  
desfeito e o client é parado — nunca fica registry/conexão órfã. O  
**graceful shutdown** (Ctrl+C / SIGTERM / SIGBREAK) encerra em poucos segundos  
sem deixar processos órfãos e sem traceback.  
  
### Namespacing  
  
Tools e prompts são expostos como `backend.nome`. Resources são endereçados por  
URI, então o prefixo é aplicado à string inteira: `file:///tmp/a.txt` do  
backend `fs` vira `fs.file:///tmp/a.txt` — continua sendo uma URI válida,  
reversível. O `resources/read` aceita a URI namespaced, chama o backend com a  
original e reescreve a resposta de volta (round-trip transparente).  
Identificadores inválidos do backend (vazios, com espaço, ou com `.` para  
tools/prompts) e duplicados na mesma listagem são rejeitados sem tocar o  
snapshot publicado.  
  
### Códigos de erro JSON-RPC  
  
| Caso | Código | HTTP |  
| --- | --- | --- |  
| JSON malformado | `-32700` ParseError | `400` |  
| Envelope inválido / campo obrigatório ausente | `-32600` InvalidRequest | `200` (corpo de erro) |  
| Method desconhecido | `-32601` MethodNotFound | `200` |  
| Parâmetro inválido de método conhecido | `-32602` InvalidParams | `200` |  
| Tool/resource/prompt namespaced inexistente | `-32001` | `200` |  
| Backend indisponível / remoto fora do ar / stream SSE caído | `-32002` | `200` |  
| Erro JSON-RPC vindo do backend | código original | repassado |  
  
Validações de transporte usam HTTP status próprio: `415` (Content-Type não é  
`application/json`), `413` (payload acima do limite), `401` (auth). Nunca um  
500 genérico. Batch JSON-RPC (array) não é suportado nesta fase → `-32600`.  
  
### Logging estruturado  
  
Todo log passa pelo `structlog`. Cada request HTTP ganha um `request_id` (UUID)  
via `contextvars`, propagado automaticamente para todos os logs daquela request  
(inclusive dentro do `McpServer` e dos clients). Os logs incluem method, id,  
backend roteado, resultado e `duration_ms`. O mesmo stream de logs alimenta o  
console ao vivo do dashboard.  
  
---  
  
## Testes  
  
```bash  
pytest  
```  
  
A suíte usa **backends fake** (stdio, HTTP e SSE, como subprocessos reais, com a  
mesma semântica de protocolo) — nunca MCPs reais de terceiros. Cobre  
registries, handlers de tools/resources/prompts, validações HTTP, consistência  
de `request_id`, ciclo de vida do `BackendManager`, detecção de queda e  
auto-restart com backoff, rotas de observabilidade e controle, dashboard,  
importador, filtro seletivo por sessão e shutdown sem processos órfãos.  
  
---  
  
## Escopo e pendências  
  
- Transporte JSON-RPC puro sobre `POST /mcp` (stateless). A dança de sessão do  
  streamable HTTP oficial e o **batch** JSON-RPC ficam para fases futuras.  
- Restart de backend stdio reusa o `command`/`args` do config (sem hot-reload).  
- O fake SSE multi-cliente com session id fica para depois (os testes usam um  
  cliente por fake).  
- **Métricas (Prometheus/OpenTelemetry): adiadas** — `/health`, `/api/servers`,  
  `/api/tools/size` e os logs estruturados cobrem a observabilidade do uso atual.  
- Adapters de auto-instalação em outros apps (Cursor, VSCode) — projeto separado.  
  
---  
  
## Licença  
  
[Distribuído sob a licença MIT](LICENSE).