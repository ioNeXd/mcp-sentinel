# Instruções de comportamento — Agente construindo o MCP Gateway

Estas são as regras de conduta para qualquer IA/agente que trabalhar neste projeto, em qualquer fase. Leia isto antes de escrever ou alterar qualquer código. Em caso de dúvida entre "rápido" e "correto conforme aqui", escolha correto conforme aqui.

## Antes de codar

- Leia `ROADMAP.md` primeiro. Toda decisão de código deve ser compatível com as fases futuras — não implemente algo que precise ser jogado fora na Fase 1 ou 2.
- Trabalhe **só na fase solicitada**. Não adiante features de fases futuras (ex: não implemente HTTP/SSE client na Fase 0, não implemente auth na Fase 0). Adiantar gera complexidade não testada.
- Se uma instrução do prompt parecer conflitar com o Roadmap, pare e sinalize o conflito antes de decidir sozinho.

## Arquitetura

- Respeite a separação de camadas já definida: `HttpServer` (transporte/validação) → `McpServer` (protocolo JSON-RPC) → `BackendManager` (roteamento) → `Client` (stdio/http/sse). Nenhuma camada deve pular a outra (ex: `HttpServer` nunca fala direto com um `Client`).
- Cada classe/módulo tem uma responsabilidade única. Se um arquivo está fazendo parsing E validação E I/O, provavelmente precisa ser dividido.
- Prefira composição a herança. Use `Protocol`/ABC apenas quando houver mais de uma implementação real (ex: `BaseClient` para Stdio/HTTP/SSE faz sentido; não crie abstrações para coisas com uma única implementação).
- Toda operação de I/O (rede, processo, disco) deve ser `async`. Nunca bloqueie o event loop.

## Código

- Use **type hints** em 100% das funções públicas (parâmetros e retorno).
- Use **Pydantic** para toda estrutura de dados que cruza uma borda (config, request/response JSON-RPC, respostas HTTP). Não use dicts soltos para dados estruturados.
- Nomes de variáveis e funções em inglês (convenção do resto do ecossistema Python/MCP), comentários e docstrings podem ser em português ou inglês — mantenha consistência dentro do arquivo.
- Docstrings no padrão Google ou NumPy (escolha um e mantenha) em toda função pública. Comentários inline só onde a lógica não é óbvia por si só (não comente o óbvio, tipo `# incrementa contador`).
- Trate exceções de forma específica — nunca `except Exception` genérico sem re-raise ou log explícito do motivo.
- Sem "magic numbers" soltos no código — use constantes nomeadas ou campos de config.

## Testes

- Toda função pública nova vem com teste (`pytest`). Sem exceção, mesmo na Fase 0.
- Para testar o `StdioClient`, crie um backend MCP fake (processo Python simples que responde `tools/list`/`tools/call` fixos) em vez de depender de um MCP real de terceiros — isso evita testes frágeis e dependências externas.
- Teste o caminho de erro, não só o caminho feliz (ex: processo que não sobe, JSON malformado, backend que não responde).
- Não avance para a próxima fase sem os testes da fase atual passando.

## Ao final de cada fase

- Rode os testes.
- Atualize o `README.md` com o que já funciona e como rodar.
- Resuma em uma mensagem curta o que foi implementado, o que ficou pendente/decidido de forma diferente do prompt original, e por quê.

## O que nunca fazer

- Não introduzir dependências novas sem necessidade clara (cada lib nova é peso de manutenção).
- Não implementar autenticação, multi-tenant, hot-reload ou dashboard antes das fases correspondentes no `ROADMAP.md`.
- Não deixar código morto ou comentado "para depois" — se não é usado, remove.
- Não assumir portas/descoberta automática de MCPs — o Gateway sempre inicia e gerencia os backends via config explícito (ver contexto no `ROADMAP.md`).
