#!/usr/bin/env bash
# Bootstrap idempotente do banco. Roda uma vez, no boot, e sai.
set -euo pipefail

echo "[migrate] criando database e aplicando opcoes de isolamento..."
python -m scripts.bootstrap

echo "[migrate] aplicando migrations (alembic)..."
alembic upgrade head

echo "[migrate] semeando dados minimos..."
python -m scripts.seed

echo "[migrate] concluido."
