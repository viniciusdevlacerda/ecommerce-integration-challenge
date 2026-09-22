from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from enum import StrEnum, auto

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from src.ingest_worker.handlers.base import DependencyNotReadyError, HandlerRegistry
from src.shared.db.uow import UnitOfWork
from src.shared.domain.events import EventEnvelope
from src.shared.messaging.dedupe import RedisDeduplicator

logger = logging.getLogger(__name__)


class Outcome(StrEnum):
    PROCESSED = auto()
    DUPLICATE = auto()
    RETRY = auto()
    REJECTED = auto()


@dataclass(frozen=True, slots=True)
class Result:
    outcome: Outcome
    detail: str = ""

    @property
    def should_ack(self) -> bool:
        return self.outcome is not Outcome.RETRY


class EventProcessor:
    """Processa um evento de forma idempotente, com tres barreiras.

    Cada uma cobre um caso que as outras nao cobrem:

      1. reserva no Redis      -> duplicata imediata; descartada sem gastar
                                  conexao com o banco;
      2. PK de processed_events -> duplicata tardia, ou worker que morreu ANTES
                                  do ACK e teve a mensagem reentregue;
      3. constraints UNIQUE     -> reenvio do mesmo fato com event_id NOVO -- o
                                  caso que escapa de quem so deduplica evento.

    A barreira 2 roda na mesma transacao do efeito de negocio: ou o evento fica
    registrado e o efeito gravado, ou nada acontece.
    """

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        registry: HandlerRegistry,
        deduplicator: RedisDeduplicator,
    ) -> None:
        self._session_factory = session_factory
        self._registry = registry
        self._deduplicator = deduplicator

    def process(self, envelope: EventEnvelope) -> Result:
        if not self._registry.supports(envelope.event_type):
            return Result(Outcome.REJECTED, f"tipo desconhecido: {envelope.event_type}")

        if not self._deduplicator.claim(envelope.event_id):
            return Result(Outcome.DUPLICATE, "descartado pelo dedupe do Redis")

        try:
            return self._apply(envelope)
        except DependencyNotReadyError as exc:
            self._deduplicator.release(envelope.event_id)
            return Result(Outcome.RETRY, str(exc))
        except Exception as exc:
            self._deduplicator.release(envelope.event_id)
            logger.exception("falha ao processar evento", extra={"event_id": envelope.event_id})
            return Result(Outcome.RETRY, str(exc))

    def _apply(self, envelope: EventEnvelope) -> Result:
        handler = self._registry.get(envelope.event_type)

        with UnitOfWork(self._session_factory) as uow:
            if not self._claim_in_ledger(uow, envelope):
                return Result(Outcome.DUPLICATE, "ja registrado em processed_events")

            try:
                handler.handle(uow, envelope)
                uow.commit()
            except IntegrityError as exc:
                uow.rollback()
                logger.info(
                    "constraint de negocio barrou duplicata",
                    extra={"event_id": envelope.event_id, "error": str(exc.orig)},
                )
                return Result(Outcome.DUPLICATE, "barrado por constraint de negocio")

        return Result(Outcome.PROCESSED)

    @staticmethod
    def _claim_in_ledger(uow: UnitOfWork, envelope: EventEnvelope) -> bool:
        try:
            uow.processed_events.register(
                event_id=envelope.event_id,
                event_type=envelope.event_type.value,
                payload_hash=_fingerprint(envelope),
            )
        except IntegrityError:
            uow.rollback()
            return False
        return True


def _fingerprint(envelope: EventEnvelope) -> str:
    return hashlib.sha256(envelope.model_dump_json().encode()).hexdigest()
