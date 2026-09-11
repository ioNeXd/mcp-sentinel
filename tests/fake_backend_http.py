#!/usr/bin/env python3  
"""Backend MCP fake via HTTP (JSON-RPC sobre POST) para os testes da Fase 3.  
  
Mesma semântica do fake stdio (``tests/fake_logic.py``), trocando só o  
transporte: cada request JSON-RPC é um POST na raiz e a resposta sai no corpo  
HTTP. Levantado como subprocesso pelos testes (client httpx real contra  
uvicorn real) e pelo smoke test manual do Gateway.  
  
Uso:  
    python tests/fake_backend_http.py --port 8931 [--delay 1.0]  
    [--no-resources] [--no-prompts] (via fake_logic)  
"""  
  
import argparse  
  
import uvicorn  
from fastapi import FastAPI, Request  
from fastapi.responses import JSONResponse  
  
import fake_logic  
  
LAST_HEADERS: dict[str, str] = {}  
"""Headers do último POST recebido, expostos em ``/last-headers`` para o teste  
de headers do config verificar o que o Gateway realmente enviou."""  
  
  
def create_app() -> FastAPI:  
    """App FastAPI com o POST JSON-RPC na raiz.  
  
    ``POST /`` processa o envelope: notificação (sem ``id``) responde ``202``  
    sem corpo — o Gateway não espera resposta JSON-RPC; request comum devolve  
    o envelope de ``fake_logic.build_response`` no corpo.  
    """  
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)  
  
    @app.post("/")  
    async def mcp(request: Request) -> JSONResponse:  
        LAST_HEADERS.clear()  
        LAST_HEADERS.update(request.headers)  
        body = await request.json()  
        if fake_logic.is_notification(body):  
            return JSONResponse(status_code=202, content=None)  
        return JSONResponse(content=fake_logic.build_response(body))  
  
    @app.get("/last-headers")  
    async def last_headers() -> JSONResponse:  
        """Expõe os headers do último POST (usado pelo teste de headers)."""  
        return JSONResponse(content=LAST_HEADERS)  
  
    return app  
  
  
def main() -> None:  
    """CLI do fake HTTP.  
  
    ``--delay`` é consumido por ``fake_logic`` via ``sys.argv``; é declarado  
    aqui apenas para o argparse não abortar com "unrecognized arguments".  
    """  
    parser = argparse.ArgumentParser(description=__doc__)  
    parser.add_argument("--port", type=int, default=8931)  
    parser.add_argument("--delay", type=float, default=0.0)  
    args, _ = parser.parse_known_args()  
    uvicorn.run(create_app(), host="127.0.0.1", port=args.port, log_level="warning")  
  
  
if __name__ == "__main__":  
    main()