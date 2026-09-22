from __future__ import annotations

from types import TracebackType

from sqlalchemy.orm import Session, sessionmaker

from src.shared.db.repositories import (
    AddressRepository,
    ClientRepository,
    InvoiceRepository,
    OrderRepository,
    OutboxRepository,
    PaymentRepository,
    ProcessedEventRepository,
)


class UnitOfWork:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory
        self.session: Session

    def __enter__(self) -> UnitOfWork:
        self.session = self._session_factory()
        self.clients = ClientRepository(self.session)
        self.addresses = AddressRepository(self.session)
        self.orders = OrderRepository(self.session)
        self.payments = PaymentRepository(self.session)
        self.invoices = InvoiceRepository(self.session)
        self.outbox = OutboxRepository(self.session)
        self.processed_events = ProcessedEventRepository(self.session)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self.session.rollback()
        self.session.close()

    def commit(self) -> None:
        self.session.commit()

    def rollback(self) -> None:
        self.session.rollback()

    def flush(self) -> None:
        self.session.flush()
