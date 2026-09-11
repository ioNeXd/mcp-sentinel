"""SessionFilter: views seletivas de backends por sessão (Fase 5 do ROADMAP).  
  
Motivação: o Gateway é consumido por um agente (OpenClaude) com modelos  
gratuitos de contexto limitado e tool-calling menos confiável. Expor todas as  
tools de todos os backends de uma vez pode estourar contexto ou confundir o  
modelo — o filtro deixa a sessão ativar só o subconjunto de backends de que  
precisa.  
  
Princípios  
----------  
- **Não duplica dados**: os registries globais continuam sendo a fonte única  
  de verdade com tudo; aqui guarda-se apenas o subconjunto de nomes de backend  
  que cada sessão ativou. A filtragem é calculada na hora de responder.  
- **Compatibilidade**: sessão sem filtro vê tudo, exatamente como antes —  
  clientes que não conhecem a extensão (Claude Desktop, ``mcp-remote``) não  
  mudam de comportamento.  
- **Isolamento**: cada sessão tem seu próprio conjunto; sessões com filtros  
  diferentes nunca interferem uma na outra.  
  
Identificação da sessão: header HTTP ``Mcp-Session-Id`` (nome alinhado ao  
streamable HTTP do MCP, reutilizado aqui como mecanismo simples e  
stateless-friendly). Sem o header, não há sessão — e sem sessão não há filtro  
(comportamento default, total).  
  
Robustez  
--------  
- **Purga periódica** (``SessionPurger``): além da limpeza oportunista (a cada  
  N escritas / por intervalo entre escritas), uma task em background remove  
  expiradas em intervalos fixos — sessões criadas e depois abandonadas (nunca  
  mais lidas nem escritas) saem da memória mesmo sem tráfego.  
- **Teto de sessões** (``max_sessions``): um cliente gerando ids únicos a cada  
  request não pode fazer ``_sessions`` crescer sem limite entre um purge e  
  outro. Ao lotar, a sessão com deadline de expiração mais antigo é descartada  
  para abrir espaço — ver ``SessionFilter._enforce_max_sessions``.  
- **session_id validado** (``is_valid_session_id``): tamanho máximo  
  (``MAX_SESSION_ID_LENGTH``) e charset restrito. Valor inválido é tratado  
  exatamente como "sem sessão" (idêntico a ``session_id is None``) — nunca é  
  aceito cru como chave de dict nem vai para o log.  
  
Concorrência  
------------  
As seções críticas de ``SessionFilter`` (leitura/escrita de ``_sessions``) são  
inteiramente síncronas, sem nenhum ``await`` no meio: o event loop asyncio as  
executa até o fim sem interleaving, então NÃO há locks. Se algum método vier a  
se tornar ``async`` com ``await`` dentro de uma seção crítica, um lock terá de  
ser introduzido junto com essa mudança.  
"""  
  
import asyncio  
import re  
import time  
from typing import Callable  
  
import structlog  
  
logger = structlog.get_logger(__name__)  
  
PURGE_EVERY_N_WRITES = 32  
"""Limpeza oportunista: a cada N escritas, remove sessões expiradas."""  
  
PURGE_INTERVAL_SECONDS = 300.0  
"""Intervalo máximo entre purgas oportunistas e período do loop do  
``SessionPurger``: uma constante só, duas frentes de limpeza no mesmo ritmo."""  
  
DEFAULT_MAX_SESSIONS = 256  
"""Teto de sessões simultâneas: cada entrada é um par (frozenset, float), então  
256 sessões ocupam poucos KB — um teto generoso que só reage a abuso."""  
  
MAX_SESSION_ID_LENGTH = 128  
"""Tamanho máximo de um session_id: é chave de dict e aparece em logs  
estruturados — valor maior é tratado como 'sem sessão', nunca aceito cru."""  
  
_SESSION_ID_PATTERN = re.compile(r"^[A-Za-z0-9._~-]+$")  
"""Charset de ids seguros em URLs/headers: ASCII sem espaços nem separadores."""  
  
SESSION_HEADER = "Mcp-Session-Id"  
  
  
def is_valid_session_id(session_id: str | None) -> bool:  
    """Indica se ``session_id`` pode ser usado como chave de sessão.  
  
    Regras: string não vazia, até ``MAX_SESSION_ID_LENGTH`` caracteres e só  
    caracteres do charset seguro (letras, números, ``.``, ``_``, ``~``, ``-``).  
  
    A rejeição acontece aqui no ``SessionFilter`` (e não na camada HTTP que lê  
    o header) porque este é o ponto único por onde toda leitura/escrita passa —  
    cobre os dois leitores do header (rota /mcp e /api/tools/size) e qualquer  
    consumidor futuro, sem duplicar a checagem em cada rota.  
    """  
    if not session_id:  
        return False  
    return (  
        len(session_id) <= MAX_SESSION_ID_LENGTH  
        and _SESSION_ID_PATTERN.match(session_id) is not None  
    )  
  
  
def normalize_session_id(session_id: str | None) -> str | None:  
    """Devolve o id utilizável como chave de sessão, ou ``None``.  
  
    ``None``/vazio/inválido (tamanho ou charset) viram ``None`` — o mesmo valor  
    de "sem sessão" — para que nenhum id cru vire chave de dict nem apareça em  
    logs. Chamado nos pontos de entrada do header (rota /mcp e /api/tools/size)  
    e no ``SessionFilter`` (defesa em profundidade, cobre chamadas diretas de  
    ``process_message``).  
    """  
    if session_id is None or not is_valid_session_id(session_id):  
        return None  
    return session_id  
  
  
class SessionFilter:  
    """Filtro de backends ativos por sessão, com expiração por inatividade.  
  
    Attributes:  
        ttl_seconds: Tempo de vida de uma sessão sem atividade. A cada acesso,  
            o TTL é renovado — sessão ativa nunca expira no meio do uso.  
        clock: Fonte de tempo monotônico (injetável para testes com mock de  
            tempo; default ``time.monotonic``).  
        max_sessions: Teto de sessões simultâneas (default  
            ``DEFAULT_MAX_SESSIONS``). Ao criar uma sessão NOVA com o teto  
            cheio, a sessão com deadline de expiração mais antigo é descartada  
            (estratégia: evição da mais antiga, não recusa — ver  
            ``_enforce_max_sessions``). Renovar uma sessão que já existe não  
            conta como nova e nunca dispara evição.  
    """  
  
    def __init__(  
        self,  
        ttl_seconds: float,  
        clock: Callable[[], float] = time.monotonic,  
        max_sessions: int = DEFAULT_MAX_SESSIONS,  
    ) -> None:  
        self.ttl_seconds = ttl_seconds  
        self.max_sessions = max_sessions  
        self._clock = clock  
        self._sessions: dict[str, tuple[frozenset[str], float]] = {}  
        self._writes_since_purge = 0  
        self._last_purge = clock()  
  
    def set_active_backends(self, session_id: str, backends: frozenset[str]) -> None:  
        """Define (ou substitui) o filtro da sessão e renova o TTL.  
  
        A validação de nomes (backends existentes, não vazios) é  
        responsabilidade do chamador (McpServer) — aqui é armazenamento puro.  
  
        ``session_id`` inválido (tamanho/charset) é tratado como "sem sessão":  
        nada é armazenado e o log registra apenas o tamanho do valor recebido,  
        nunca o valor em si (pode ser header arbitrário de um cliente).  
  
        Só abre espaço via ``_enforce_max_sessions`` quando a escrita CRIA uma  
        sessão nova; renovar uma existente não cresce o dict e não evicta  
        ninguém.  
        """  
        if not is_valid_session_id(session_id):  
            logger.warning(  
                "session_id_invalido_ignorado",  
                session_id_size=len(session_id) if session_id else 0,  
            )  
            return  
        now = self._clock()  
        if session_id not in self._sessions:  
            self._enforce_max_sessions(now)  
        self._sessions[session_id] = (frozenset(backends), now + self.ttl_seconds)  
        self._maybe_purge(now)  
        logger.info(  
            "session_filter_set",  
            session_id=session_id,  
            backends=sorted(backends),  
            ttl_seconds=self.ttl_seconds,  
        )  
  
    def clear(self, session_id: str) -> None:  
        """Remove o filtro da sessão (volta a ver todos os backends)."""  
        self._sessions.pop(session_id, None)  
        logger.info("session_filter_cleared", session_id=session_id)  
  
    def active_backends(self, session_id: str | None) -> frozenset[str] | None:  
        """Conjunto de backends da sessão, ou ``None`` se não há filtro.  
  
        O acesso RENOVA o TTL (sessão em uso não expira). Sessão expirada é  
        removida no próprio acesso — da perspectiva do chamador, deixa de  
        existir e a sessão volta a ver tudo (comportamento seguro: o default é  
        sempre ver tudo).  
  
        Id inválido (tamanho/charset) devolve ``None``, exatamente como  
        ``session_id is None``: nenhum valor cru vira chave de leitura.  
        """  
        if session_id is None or not is_valid_session_id(session_id):  
            return None  
        entry = self._sessions.get(session_id)  
        if entry is None:  
            return None  
        backends, deadline = entry  
        now = self._clock()  
        if now >= deadline:  
            del self._sessions[session_id]  
            logger.info("session_expired", session_id=session_id, ttl_seconds=self.ttl_seconds)  
            return None  
        self._sessions[session_id] = (backends, now + self.ttl_seconds)  
        return backends  
  
    def purge_expired(self) -> int:  
        """Remove todas as sessões expiradas; devolve quantas foram removidas."""  
        now = self._clock()  
        expired = [sid for sid, (_, deadline) in self._sessions.items() if now >= deadline]  
        for sid in expired:  
            del self._sessions[sid]  
        if expired:  
            logger.info("sessions_purged", count=len(expired))  
        self._last_purge = now  
        self._writes_since_purge = 0  
        return len(expired)  
  
    def session_count(self) -> int:  
        """Número de sessões com filtro armazenadas (uso em diagnóstico)."""  
        return len(self._sessions)  
  
    def _maybe_purge(self, now: float) -> None:  
        """Limpeza oportunista: por contagem de escritas ou por intervalo."""  
        self._writes_since_purge += 1  
        if (  
            self._writes_since_purge >= PURGE_EVERY_N_WRITES  
            or (now - self._last_purge) >= PURGE_INTERVAL_SECONDS  
        ):  
            self.purge_expired()  
  
    def _enforce_max_sessions(self, now: float) -> None:  
        """Aplica o teto de sessões: ao lotar, descarta a de deadline mais antigo.  
  
        Estratégia: **evição da sessão mais próxima de expirar** (deadline mais  
        antigo) em vez de recusar a nova sessão. Recusar derrubaria o fluxo de  
        um cliente legítimo por causa de lixo de outro (ex.: ids únicos gerados  
        a cada request); evictar a mais antiga ataca primeiro quem está há mais  
        tempo sem renovação — exatamente o perfil do abuso (ids nunca  
        reutilizados) — e no limite é quase o que a expiração natural faria, só  
        que mais cedo. O aviso logado traz apenas contagens, nunca os ids.  
        """  
        overflow = len(self._sessions) - self.max_sessions + 1  
        if overflow <= 0:  
            return  
        by_deadline = sorted(self._sessions.items(), key=lambda kv: kv[1][1])  
        evicted = 0  
        for sid, _ in by_deadline[:overflow]:  
            del self._sessions[sid]  
            evicted += 1  
        logger.warning(  
            "session_evicted_max_sessions",  
            count=evicted,  
            sessions=len(self._sessions),  
            max_sessions=self.max_sessions,  
        )  
  
  
class SessionPurger:  
    """Purga periódica de sessões expiradas, independente de tráfego.  
  
    Complemento (não substituto) da limpeza oportunista do ``SessionFilter``:  
    sessões criadas e depois abandonadas — nunca mais lidas nem escritas — só  
    sairiam da memória se uma escrita nova disparasse o purge oportunista. Este  
    loop remove expiradas a cada ``interval_seconds`` (default  
    ``PURGE_INTERVAL_SECONDS``, a mesma constante do gatilho oportunista) mesmo  
    com tráfego zero.  
  
    Mesmo padrão de lifecycle do ``HealthMonitor``: ``start()`` cria a task no  
    loop em execução; ``stop()`` cancela e aguarda (idempotente).  
    """  
  
    def __init__(  
        self, sessions: SessionFilter, interval_seconds: float = PURGE_INTERVAL_SECONDS  
    ) -> None:  
        self._sessions = sessions  
        self._interval_seconds = interval_seconds  
        self._task: asyncio.Task[None] | None = None  
  
    def start(self) -> None:  
        """Inicia o loop em background (idempotente)."""  
        if self._task is None or self._task.done():  
            self._task = asyncio.create_task(self._run(), name="session-purger")  
  
    async def stop(self) -> None:  
        """Cancela o loop e aguarda a task terminar (idempotente).  
  
        O ``CancelledError`` do cancelamento é o desfecho esperado e é  
        suprimido; qualquer outra exceção é um erro real e propaga (mesmo  
        contrato do ``HealthMonitor.stop``). Se o próprio ``stop()`` for  
        cancelado, o cancelamento do chamador propaga após o cleanup.  
        """  
        task = self._task  
        if task is not None:  
            self._task = None  
            task.cancel()  
            results = await asyncio.gather(task, return_exceptions=True)  
            result = results[0] if results else None  
            if isinstance(result, asyncio.CancelledError):  
                pass  
            elif isinstance(result, BaseException):  
                raise result  
        logger.info("session_purger_stopped")  
  
    async def _run(self) -> None:  
        """Loop principal: purga, dorme o intervalo, repete.  
  
        A purga roda ANTES do primeiro sleep (mesma ordem do ``HealthMonitor``):  
        no startup ela é um no-op barato, e o primeiro ciclo nunca fica preso ao  
        intervalo inteiro. ``CancelledError`` propaga (é o mecanismo de parada  
        do ``stop()``); qualquer outro erro é logado e o loop continua.  
        """  
        logger.info("session_purger_started", interval_seconds=self._interval_seconds)  
        while True:  
            try:  
                self._sessions.purge_expired()  
                await asyncio.sleep(self._interval_seconds)  
            except asyncio.CancelledError:  
                raise  
            except Exception:  
                logger.exception("session_purger_loop_error")