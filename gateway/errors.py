"""Exceções do domínio do Gateway.  
  
Hierarquia enraizada em ``BackendError``: qualquer falha de comunicação com um  
backend MCP é subtipo dela, então um ``except BackendError`` captura todos os  
casos. As subclasses carregam contexto estruturado (``backend``, ``status_code``,  
``code``/``data``) para permitir tratamento programático quando necessário.  
"""  
  
from typing import Any  
  
  
class BackendError(Exception):  
    """Erro genérico de comunicação com um backend MCP."""  
  
  
class BackendNotFoundError(BackendError):  
    """Backend não existe na configuração do Gateway."""  
  
  
class BackendStateConflictError(BackendError):  
    """Operação incompatível com o estado atual do backend."""  
  
  
class BackendTimeoutError(BackendError):  
    """Backend não respondeu dentro do timeout configurado."""  
  
  
class BackendDisconnectedError(BackendError):  
    """Processo do backend caiu ou foi encerrado.  
  
    ``backend`` guarda o nome do backend afetado (quando conhecido) para logs e  
    diagnóstico sem depender de parsing da mensagem.  
    """  
  
    def __init__(self, message: str, *, backend: str | None = None) -> None:  
        super().__init__(message)  
        self.backend = backend  
  
  
class BackendHttpStatusError(BackendDisconnectedError):  
    """Backend respondeu HTTP com código de erro (>= 400).  
  
    Subclasse de ``BackendDisconnectedError`` para compatibilidade com quem  
    trata o tipo genérico, mas expõe ``status_code`` e ``method`` para permitir  
    diferenciar programaticamente uma resposta 4xx (ex.: 401) de uma queda de  
    rede.  
    """  
  
    def __init__(self, status_code: int, method: str, *, backend: str | None = None) -> None:  
        origem = backend or "backend desconhecido"  
        super().__init__(f"backend '{origem}': HTTP {status_code} em '{method}'", backend=backend)  
        self.status_code = status_code  
        self.method = method  
  
  
class BackendJsonRpcError(BackendError):  
    """Backend respondeu com um erro JSON-RPC.  
  
    Carrega ``code``, ``message`` e ``data`` originais do envelope JSON-RPC para  
    que o Gateway possa repropagá-los fielmente ao cliente.  
    """  
  
    def __init__(self, code: int, message: str, data: Any = None) -> None:  
        super().__init__(message)  
        self.code = code  
        self.message = message  
        self.data = data