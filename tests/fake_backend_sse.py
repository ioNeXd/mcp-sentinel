#!/usr/bin/env python3
"""Backend MCP fake via SSE (stream GET / + POST no endpoint anunciado).

Padrão de transporte consumido pelo ``SseClient`` (spec HTTP+SSE do MCP):

- ``GET /`` abre o stream ``text/event-stream``. O PRIMEIRO evento é o
  anúncio ``endpoint``: ``event: endpoint`` + ``data:
  /messages?session_id=<id>`` — a URL que o cliente deve usar para TODOS os
  POSTs daquela conexão. Como servidores reais, o fake VALIDA o session_id e
  responde ``404`` para POSTs fora da URL anunciada (é exatamente o erro que
  servidores de verdade davam para o cliente antigo, que POSTava na rota
  fixa ``/messages``);
- respostas JSON-RPC saem como eventos ``data: {...}`` no stream;
- keep-alives (``: keep-alive``) periódicos mantêm a conexão viva.

Flags (para exercitar caminhos do SseClient nos testes):

- ``--no-endpoint``: NÃO emite o evento ``endpoint`` (modo legado — o
  cliente deve cair para a rota fixa ``/messages``; POSTs sem session_id
  são aceitos);
- ``--silent``: o stream abre e NUNCA manda nada (nem endpoint nem
  keep-alive) — exercita o timeout/erro claro do cliente;
- ``--keepalive N``: intervalo dos keep-alives em segundos (default 1.0);
- ``--delay N``: atrasa as respostas (via fake_logic).

Uso:
    python tests/fake_backend_sse.py --port 8932 [--delay 1.0]
        [--no-endpoint] [--silent] [--keepalive 1.0]

Limitação conhecida (suficiente para os testes, que usam um cliente por
fake): as respostas são espalhadas para todas as conexões abertas — com um
único cliente do Gateway por backend isso é exato; com dois clientes
simultâneos os ids se colidiriam.
"""

import argparse
import asyncio
import contextlib
import json
import uuid
from typing import Any, AsyncIterator

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

import fake_logic

KEEPALIVE_INTERVAL_SECONDS = 1.0
MESSAGES_PATH = "/messages"


def create_app(
    *, send_endpoint: bool = True, silent: bool = False, keepalive: float = KEEPALIVE_INTERVAL_SECONDS
) -> FastAPI:
    """App FastAPI com o stream SSE (GET /) e o POST no endpoint anunciado."""
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    # Uma fila por conexão de stream aberta; limpa quando a conexão cai.
    queues: list[asyncio.Queue[str]] = []
    # session_ids das conexões de stream vivas (validação do POST, modo
    # com endpoint — espelha servidores reais que amarram POST à sessão).
    sessions: set[str] = set()

    @app.get("/")
    async def sse() -> StreamingResponse:
        queue: asyncio.Queue[str] = asyncio.Queue()
        queues.append(queue)
        session_id = uuid.uuid4().hex[:8]
        sessions.add(session_id)

        async def event_stream() -> AsyncIterator[str]:
            try:
                if silent:
                    # Stream aberto e mudo: exercita o timeout do cliente.
                    await asyncio.Event().wait()
                    return
                if send_endpoint:
                    # PRIMEIRO evento do stream: anúncio do endpoint (spec
                    # HTTP+SSE do MCP), com session_id próprio da conexão.
                    yield f"event: endpoint\ndata: {MESSAGES_PATH}?session_id={session_id}\n\n"
                while True:
                    try:
                        data = await asyncio.wait_for(queue.get(), timeout=keepalive)
                        yield f"data: {data}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keep-alive\n\n"
            finally:
                with contextlib.suppress(ValueError):
                    queues.remove(queue)
                sessions.discard(session_id)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.post(MESSAGES_PATH)
    async def messages(request: Request) -> JSONResponse:
        if send_endpoint:
            # Sem o session_id anunciado (ou com um expirado): 404, como
            # servidores MCP-SSE reais fazem com a rota errada.
            if request.query_params.get("session_id") not in sessions:
                return JSONResponse(
                    status_code=404,
                    content={"detail": "session_id ausente/desconhecido — use o evento 'endpoint'"},
                )
        body: Any = await request.json()
        if fake_logic.is_notification(body):
            return JSONResponse(content={"received": True})
        response = fake_logic.build_response(body)
        payload = json.dumps(response)
        for queue in queues:
            queue.put_nowait(payload)
        return JSONResponse(content={"received": True})

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8932)
    # Consumido por fake_logic via sys.argv; declarado aqui para o argparse
    # não abortar com "unrecognized arguments".
    parser.add_argument("--delay", type=float, default=0.0)
    parser.add_argument("--keepalive", type=float, default=KEEPALIVE_INTERVAL_SECONDS)
    parser.add_argument("--no-endpoint", action="store_true", help="não anuncia o endpoint")
    parser.add_argument("--silent", action="store_true", help="stream mudo (nada é enviado)")
    args, _ = parser.parse_known_args()
    uvicorn.run(
        create_app(
            send_endpoint=not args.no_endpoint,
            silent=args.silent,
            keepalive=args.keepalive,
        ),
        host="127.0.0.1",
        port=args.port,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
