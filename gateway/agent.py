"""Terminal do agente: estado da conversa + interface plugável (Fase 8).

Este módulo é DELIBERADAMENTE burro por enquanto — a inteligência do agente
em si (conectar num modelo real, dar acesso às tools agregadas do Gateway)
foi propositalmente adiada. O que existe aqui é só a fundação pra isso
encaixar depois sem precisar reescrever a interface do dashboard:

- ``AgentBackend`` é o contrato que qualquer implementação futura (um modelo
  via OpenRouter, um processo local, o que for) precisa cumprir: um método
  ``respond(message, history) -> str``.
- ``StubAgentBackend`` é a implementação atual — sempre devolve uma resposta
  fixa explicando que ainda não há modelo conectado. Trocar por um backend de
  verdade é só implementar ``AgentBackend`` e passar a instância pra
  ``AgentConversation`` (ou pra ``create_app``) — nada na rota HTTP nem no
  JavaScript do dashboard precisa mudar.
- ``AgentConversation`` guarda o histórico da sessão em memória (lista de
  mensagens). Não persiste em disco — reinicia o Gateway, reinicia a
  conversa. Isso é suficiente pra uma interface single-session; vira o
  ponto de extensão natural se um dia a "sala de conversa" (multi-sessão)
  for implementada, sem precisar mexer no contrato do ``AgentBackend``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx
import structlog

logger = structlog.get_logger(__name__)

REQUEST_TIMEOUT_SECONDS = 60.0
"""Timeout generoso: agentes de verdade (não só um modelo cru) podem demorar
mais que uma chamada simples de LLM — o OpenClaw/OpenHands/Hermes podem estar
rodando tools por trás antes de responder."""


@dataclass(frozen=True)
class AgentMessage:
    """Uma mensagem na conversa com o agente.

    Attributes:
        role: ``"user"`` ou ``"agent"``. Espelha o vocabulário comum de chat
            (não usamos "assistant" pra não confundir com o wording do
            protocolo MCP em outras partes do Gateway).
        content: Texto da mensagem.
        timestamp: Relógio de parede (``time.time()``) de quando foi
            registrada — útil pro futuro histórico/timeline, não usado ainda
            pela UI atual.
    """

    role: str
    content: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, object]:
        return {"role": self.role, "content": self.content, "timestamp": self.timestamp}


class AgentBackend(Protocol):
    """Contrato que qualquer implementação futura de agente precisa cumprir.

    Deliberadamente mínimo: recebe a mensagem nova e o histórico anterior (já
    sem a mensagem nova), devolve o texto de resposta. Streaming, tool calls,
    seleção de modelo etc. são decisões de implementação de quem cumprir este
    contrato — não vazam pra cá nem pra rota HTTP.
    """

    async def respond(self, message: str, history: list[AgentMessage]) -> str: ...

    @property
    def is_connected(self) -> bool:
        """Se este backend fala com um modelo/agente de verdade.

        A rota HTTP usa isso só para marcar a resposta como "stub" ou não no
        JSON (``model_connected``) — o dashboard usa esse campo pra estilizar
        a bolha de forma diferente enquanto não há modelo real.
        """
        ...


class StubAgentBackend:
    """Implementação atual: sem modelo real, só confirma que a interface existe.

    Isso existe pra o terminal do agente já ser testável no dashboard (envia
    mensagem, recebe resposta, limpa conversa) antes de qualquer decisão
    sobre QUAL modelo/agente conectar — a decisão de modelo fica
    completamente desacoplada desta interface.
    """

    is_connected = False

    async def respond(self, message: str, history: list[AgentMessage]) -> str:
        return (
            "Ainda não há um modelo ou agente conectado aqui — isso é só a "
            "interface do terminal, preparada para quando você configurar "
            "qual agente/modelo quer usar. Sua mensagem foi recebida: "
            f"\"{message}\""
        )


class OpenAICompatibleAgentBackend:
    """Fala com qualquer destino que exponha ``POST {base_url}/chat/completions``.

    Esse formato (payload ``{"model": ..., "messages": [...]}``, resposta com
    ``choices[0].message.content``) é o mesmo que OpenRouter, servidores
    locais (Ollama/LM Studio) e vários agentes prontos (OpenClaw, OpenHands,
    Hermes Agent) já falam — então uma implementação só cobre todos esses
    casos. Trocar de agente/modelo é só reconfigurar ``AgentProviderConfig``
    (``gateway/config.py``), sem tocar nesta classe.

    O histórico é reenviado por completo a cada chamada (sem estado guardado
    no provider) — o contrato ``AgentBackend.respond`` já recebe o histórico
    prévio, então cada request já parte com o contexto certo.
    """

    is_connected = True

    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._api_key = api_key
        self._extra_headers = extra_headers or {}

    async def respond(self, message: str, history: list[AgentMessage]) -> str:
        messages = [{"role": m.role if m.role == "user" else "assistant", "content": m.content} for m in history]
        messages.append({"role": "user", "content": message})

        headers = {"Content-Type": "application/json", **self._extra_headers}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"

        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    f"{self._base_url}/chat/completions",
                    headers=headers,
                    json={"model": self._model, "messages": messages},
                )
                response.raise_for_status()
                body: Any = response.json()
        except httpx.TimeoutException as exc:
            logger.error("agent_provider_timeout", base_url=self._base_url, error=str(exc))
            return f"O agente configurado ({self._base_url}) não respondeu a tempo ({REQUEST_TIMEOUT_SECONDS:.0f}s)."
        except httpx.HTTPStatusError as exc:
            logger.error(
                "agent_provider_http_error",
                base_url=self._base_url,
                status_code=exc.response.status_code,
            )
            return f"O agente configurado devolveu HTTP {exc.response.status_code}. Confira a API key e o modelo no config.json."
        except httpx.HTTPError as exc:
            logger.error("agent_provider_network_error", base_url=self._base_url, error=str(exc))
            return f"Não consegui conectar no agente configurado ({self._base_url}): {exc}"

        try:
            return str(body["choices"][0]["message"]["content"])
        except (KeyError, IndexError, TypeError):
            logger.error("agent_provider_malformed_response", base_url=self._base_url, body=body)
            return "O agente respondeu num formato inesperado — confira os logs pra ver o corpo bruto."


class AgentConversation:
    """Guarda o histórico da conversa e delega respostas ao ``AgentBackend``.

    Não thread-safe/lock-free por design: assume um único consumidor (a
    sessão local do dashboard) por processo — consistente com o app ser de
    uso pessoal, não multiusuário concorrente.
    """

    def __init__(self, backend: AgentBackend | None = None) -> None:
        self.backend: AgentBackend = backend or StubAgentBackend()
        self._history: list[AgentMessage] = []

    def history(self) -> list[dict[str, object]]:
        return [m.to_dict() for m in self._history]

    def clear(self) -> None:
        self._history = []

    async def send(self, message: str) -> AgentMessage:
        """Registra a mensagem do usuário, obtém a resposta, registra e devolve."""
        user_message = AgentMessage(role="user", content=message)
        history_before = list(self._history)
        self._history.append(user_message)
        reply_text = await self.backend.respond(message, history_before)
        reply_message = AgentMessage(role="agent", content=reply_text)
        self._history.append(reply_message)
        return reply_message
