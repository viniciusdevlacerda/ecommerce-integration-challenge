from __future__ import annotations

import logging
import random
import sys

from src.shared.config import get_settings
from src.shared.db.engine import get_session_factory
from src.shared.db.uow import UnitOfWork
from src.shared.logging import configure_logging

logger = logging.getLogger(__name__)

FIRST = ("Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela", "Henrique")
LAST = ("Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Almeida")
CITIES = (
    ("SP", "Sao Paulo"), ("PR", "Curitiba"), ("SC", "Joinville"),
    ("MG", "Belo Horizonte"), ("RS", "Porto Alegre"), ("DF", "Brasilia"),
)

SEED_SIZE = 20


def _cpf(rng: random.Random) -> str:
    base = [rng.randint(0, 9) for _ in range(9)]

    def digit(numbers: list[int]) -> int:
        weight = len(numbers) + 1
        total = sum(n * (weight - i) for i, n in enumerate(numbers))
        remainder = (total * 10) % 11
        return 0 if remainder == 10 else remainder

    base.append(digit(base))
    base.append(digit(base))
    return "".join(map(str, base))


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    factory = get_session_factory(settings)
    rng = random.Random(42)

    created = 0
    with UnitOfWork(factory) as uow:
        for i in range(1, SEED_SIZE + 1):
            client_uid = f"SEED-CLI-{i:05d}"
            if uow.clients.get_by_uid(client_uid) is not None:
                continue

            state, city = rng.choice(CITIES)
            client = uow.clients.upsert(
                client_uid=client_uid,
                full_name=f"{rng.choice(FIRST)} {rng.choice(LAST)}",
                document=_cpf(rng),
                email=f"cliente{i:05d}@exemplo.com.br",
            )
            uow.addresses.upsert_version(
                client_id=client.client_id,
                payload={
                    "address_uid": f"SEED-ADDR-{i:05d}",
                    "street": "Avenida das Industrias",
                    "number": str(100 + i),
                    "complement": None,
                    "district": "Centro",
                    "city": city,
                    "state": state,
                    "zip_code": f"{rng.randint(10000000, 99999999)}",
                },
            )
            created += 1
        uow.commit()

    logger.info("seed concluido", extra={"clientes_criados": created})


if __name__ == "__main__":
    sys.exit(main())
