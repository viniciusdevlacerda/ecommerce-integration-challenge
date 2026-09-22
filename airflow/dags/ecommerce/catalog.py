from __future__ import annotations

import random
from typing import Final, NamedTuple


class Product(NamedTuple):
    sku: str
    name: str
    category: str
    min_price: float
    max_price: float


PRODUCTS: Final[tuple[Product, ...]] = (
    Product("SKU-AF-4000", "Air Fryer Digital 4L", "Eletroportateis", 329.90, 549.90),
    Product("SKU-AF-8000", "Air Fryer Familia 8L", "Eletroportateis", 499.90, 799.90),
    Product("SKU-LQ-1200", "Liquidificador 1200W 12 Vel.", "Eletroportateis", 149.90, 289.90),
    Product("SKU-LQ-0800", "Liquidificador Compacto 800W", "Eletroportateis", 99.90, 179.90),
    Product("SKU-BT-0500", "Batedeira Planetaria 500W", "Eletroportateis", 279.90, 459.90),
    Product("SKU-CF-0750", "Cafeteira Eletrica 30 Xicaras", "Eletroportateis", 129.90, 249.90),
    Product("SKU-SD-0750", "Sanduicheira Grill Antiaderente", "Eletroportateis", 89.90, 159.90),
    Product("SKU-PE-0900", "Panela Eletrica de Arroz 1.8L", "Eletroportateis", 169.90, 279.90),
    Product("SKU-VT-0040", "Ventilador de Coluna 40cm", "Ventilacao", 199.90, 349.90),
    Product("SKU-VT-0030", "Ventilador de Mesa 30cm", "Ventilacao", 119.90, 199.90),
    Product("SKU-CR-0220", "Circulador de Ar 6 Pas", "Ventilacao", 179.90, 299.90),
    Product("SKU-PA-0220", "Purificador de Agua Refrigerado", "Agua", 649.90, 1099.90),
    Product("SKU-FE-0900", "Ferro a Vapor Ceramico", "Cuidado com a Roupa", 89.90, 179.90),
    Product("SKU-AS-1400", "Aspirador de Po 1400W", "Limpeza", 249.90, 429.90),
    Product("SKU-MO-0020", "Micro-ondas 20L Espelhado", "Linha Branca", 599.90, 899.90),
    Product("SKU-FG-0004", "Fogao 4 Bocas Inox", "Linha Branca", 899.90, 1499.90),
    Product("SKU-CK-0005", "Cooktop 5 Bocas Vidro Temperado", "Linha Branca", 749.90, 1199.90),
    Product("SKU-FR-0300", "Frigobar 120L", "Linha Branca", 1099.90, 1699.90),
)

CHANNELS: Final[tuple[str, ...]] = ("SITE", "MARKETPLACE", "APP", "TELEVENDAS", "B2B")
PAYMENT_METHODS: Final[tuple[str, ...]] = ("PIX", "CREDIT_CARD", "BOLETO")
ORDER_STATUSES: Final[tuple[str, ...]] = ("APPROVED", "INVOICED", "CANCELLED", "PENDING")

LOCATIONS: Final[tuple[tuple[str, str, str], ...]] = (
    ("SP", "Sao Paulo", "01"), ("SP", "Campinas", "13"), ("SP", "Santo Andre", "09"),
    ("RJ", "Rio de Janeiro", "20"), ("RJ", "Niteroi", "24"),
    ("MG", "Belo Horizonte", "30"), ("MG", "Uberlandia", "38"),
    ("PR", "Curitiba", "80"), ("PR", "Londrina", "86"),
    ("SC", "Joinville", "89"), ("SC", "Florianopolis", "88"),
    ("RS", "Porto Alegre", "90"), ("BA", "Salvador", "40"),
    ("PE", "Recife", "50"), ("CE", "Fortaleza", "60"),
    ("GO", "Goiania", "74"), ("DF", "Brasilia", "70"),
    ("ES", "Vitoria", "29"), ("PA", "Belem", "66"), ("AM", "Manaus", "69"),
)

STREETS: Final[tuple[str, ...]] = (
    "Rua das Industrias", "Avenida Brasil", "Rua Sete de Setembro", "Avenida Paulista",
    "Rua XV de Novembro", "Avenida Getulio Vargas", "Rua Marechal Deodoro",
    "Avenida das Nacoes", "Rua Santos Dumont", "Travessa do Comercio",
)

FIRST_NAMES: Final[tuple[str, ...]] = (
    "Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela", "Henrique",
    "Isabela", "Joao", "Larissa", "Marcos", "Natalia", "Otavio", "Patricia",
    "Rafael", "Sabrina", "Thiago", "Vanessa", "William",
)

LAST_NAMES: Final[tuple[str, ...]] = (
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Almeida",
    "Costa", "Gomes", "Martins", "Araujo", "Ribeiro", "Carvalho", "Lima",
)


def generate_cpf(rng: random.Random) -> str:
    base = [rng.randint(0, 9) for _ in range(9)]

    def _digit(numbers: list[int]) -> int:
        weight = len(numbers) + 1
        total = sum(n * (weight - i) for i, n in enumerate(numbers))
        remainder = (total * 10) % 11
        return 0 if remainder == 10 else remainder

    base.append(_digit(base))
    base.append(_digit(base))
    return "".join(map(str, base))


def full_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)} {rng.choice(LAST_NAMES)}"
