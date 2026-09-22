#!/usr/bin/env bash
#
# Sobe a stack completa, espera tudo ficar pronto e roda a demonstracao.
#
#   ./run.sh              sobe tudo e roda a demo
#   ./run.sh demo         roda so a demo (stack ja no ar)
#   ./run.sh verify       resumo do estado do banco e da fila
#   ./run.sh test         testes unitarios e de integracao
#   ./run.sh load         teste de carga na API de ingestao
#   ./run.sh dag [linhas] dispara a carga em massa (padrao 10000000)
#   ./run.sh bench        compara as estrategias de escrita no SQL Server
#   ./run.sh status       estado dos containers
#   ./run.sh logs [svc]   acompanha os logs
#   ./run.sh down         para a stack
#   ./run.sh reset        para e apaga os volumes
#
set -euo pipefail

cd "$(dirname "$0")"

PORTS="1433 6379 8000 8001 8080"
MIGRATE_TIMEOUT=900
HEALTH_TIMEOUT=180
AIRFLOW_TIMEOUT=300

if [ -t 1 ]; then
    BOLD=$(printf '\033[1m'); RED=$(printf '\033[31m')
    GREEN=$(printf '\033[32m'); YELLOW=$(printf '\033[33m')
    DIM=$(printf '\033[2m'); OFF=$(printf '\033[0m')
else
    BOLD=""; RED=""; GREEN=""; YELLOW=""; DIM=""; OFF=""
fi

step()  { printf '\n%s==>%s %s%s%s\n' "$BOLD" "$OFF" "$BOLD" "$1" "$OFF"; }
ok()    { printf '    %sok%s  %s\n' "$GREEN" "$OFF" "$1"; }
warn()  { printf '    %s!%s   %s\n' "$YELLOW" "$OFF" "$1"; }
fail()  { printf '\n    %serro%s  %s\n\n' "$RED" "$OFF" "$1"; exit 1; }
note()  { printf '    %s%s%s\n' "$DIM" "$1" "$OFF"; }

# --------------------------------------------------------------- pre-requisitos
detect_compose() {
    if docker compose version >/dev/null 2>&1; then
        COMPOSE="docker compose"
    elif docker-compose version >/dev/null 2>&1; then
        COMPOSE="docker-compose"
    else
        fail "Docker Compose nao encontrado. Instale o Docker Desktop ou o OrbStack."
    fi
}

check_docker() {
    command -v docker >/dev/null 2>&1 || fail "Docker nao encontrado no PATH."
    docker info >/dev/null 2>&1 || fail "O Docker nao esta rodando. Abra o Docker Desktop (ou o OrbStack) e tente de novo."
    detect_compose
    ok "Docker disponivel ($COMPOSE)"
}

check_ports() {
    busy=""
    for port in $PORTS; do
        if command -v lsof >/dev/null 2>&1; then
            lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 && busy="$busy $port"
        fi
    done
    if [ -n "$busy" ]; then
        warn "portas ja em uso:$busy"
        note "libere esses processos ou ajuste as portas no docker-compose.yml"
    else
        ok "portas livres ($PORTS)"
    fi
}

check_env() {
    if [ ! -f .env ]; then
        cp .env.example .env
        ok ".env criado a partir do .env.example"
    else
        ok ".env presente"
    fi
}

# ------------------------------------------------------------------ subida
container_id() { $COMPOSE ps -aq "$1" 2>/dev/null | head -1; }

wait_for_migrate() {
    printf '    aguardando as migrations'
    waited=0
    while [ "$waited" -lt "$MIGRATE_TIMEOUT" ]; do
        cid=$(container_id migrate)
        if [ -n "$cid" ]; then
            state=$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || echo "")
            if [ "$state" = "exited" ]; then
                code=$(docker inspect -f '{{.State.ExitCode}}' "$cid")
                printf '\n'
                if [ "$code" = "0" ]; then
                    ok "schema criado e migrations aplicadas"
                    return 0
                fi
                printf '\n'
                $COMPOSE logs --no-color migrate | tail -30
                fail "as migrations falharam (codigo $code). O log acima mostra o motivo."
            fi
        fi
        printf '.'
        sleep 3
        waited=$((waited + 3))
    done
    printf '\n'
    fail "as migrations nao terminaram em ${MIGRATE_TIMEOUT}s."
}

wait_for_api() {
    printf '    aguardando a API de ingestao'
    waited=0
    while [ "$waited" -lt "$HEALTH_TIMEOUT" ]; do
        cid=$(container_id webhook-api)
        if [ -n "$cid" ]; then
            health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$cid" 2>/dev/null || echo "")
            if [ "$health" = "healthy" ]; then
                printf '\n'
                ok "API respondendo"
                return 0
            fi
        fi
        printf '.'
        sleep 3
        waited=$((waited + 3))
    done
    printf '\n'
    fail "a API nao ficou saudavel em ${HEALTH_TIMEOUT}s. Veja: $COMPOSE logs webhook-api"
}

wait_for_airflow() {
    printf '    aguardando o Airflow'
    waited=0
    while [ "$waited" -lt "$AIRFLOW_TIMEOUT" ]; do
        cid=$(container_id airflow)
        if [ -n "$cid" ]; then
            state=$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || echo "")
            if [ "$state" = "exited" ]; then
                printf '\n'
                $COMPOSE logs --no-color airflow | tail -20
                fail "o Airflow parou. O log acima mostra o motivo."
            fi
            health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$cid" 2>/dev/null || echo "")
            if [ "$health" = "healthy" ]; then
                printf '\n'
                ok "Airflow disponivel"
                return 0
            fi
        fi
        printf '.'
        sleep 5
        waited=$((waited + 5))
    done
    printf '\n'
    warn "o Airflow ainda nao respondeu em ${AIRFLOW_TIMEOUT}s"
    note "o restante da stack funciona; acompanhe com ./run.sh logs airflow"
}

bring_up() {
    step "1/4  Verificando o ambiente"
    check_docker
    check_ports
    check_env

    step "2/4  Construindo as imagens e subindo os servicos"
    note "a primeira execucao leva de 5 a 10 minutos (driver ODBC + SQL Server)"
    set +e
    $COMPOSE up -d --build
    up_status=$?
    set -e
    if [ "$up_status" != "0" ] && [ -z "$(container_id migrate)" ]; then
        fail "a subida falhou antes de criar os containers. A saida acima mostra o motivo."
    fi

    step "3/4  Esperando o banco ficar pronto"
    wait_for_migrate
    wait_for_api
    wait_for_airflow
    sleep 5
}

summary() {
    printf '\n%s%s%s\n' "$BOLD" "Stack no ar" "$OFF"
    printf '    API de webhooks ... http://localhost:8000/docs\n'
    printf '    ERP ficticio ...... http://localhost:8001/docs\n'
    printf '    Airflow ........... http://localhost:8080   (admin / admin)\n'
    printf '    SQL Server ........ localhost:1433          (sa / ver .env, base ecommerce_ops)\n'
    printf '\n%sPara continuar%s\n' "$BOLD" "$OFF"
    printf '    ./run.sh test     testes automatizados\n'
    printf '    ./run.sh dag      carga de 10 milhoes de linhas\n'
    printf '    ./run.sh load     teste de carga na ingestao\n'
    printf '    ./run.sh down     para tudo\n\n'
}

# ------------------------------------------------------------------ acoes
run_demo() {
    step "4/4  Demonstracao ponta a ponta"
    $COMPOSE exec -T webhook-api python -m scripts.demo
}

run_verify() {
    step "Estado do sistema"
    $COMPOSE exec -T webhook-api python -m scripts.verify
}

run_tests() {
    step "Testes unitarios"
    $COMPOSE run --rm --no-deps -T webhook-api pytest -m "not integration" -q
    step "Testes de integracao"
    $COMPOSE exec -T webhook-api pytest -m integration -q
}

run_load() {
    step "Carga na API de ingestao"
    note "respostas 429 sao o backpressure atuando, nao falha"
    $COMPOSE exec -T webhook-api python -m scripts.loadtest --total 50000 --concurrency 128
}

run_dag() {
    rows="${1:-10000000}"
    step "Disparando a carga em massa ($rows linhas)"
    $COMPOSE exec -T airflow \
        airflow dags trigger load_order_history_bulk --conf "{\"row_count\": $rows}"
    note "acompanhe em http://localhost:8080 (admin / admin)"
}

run_bench() {
    step "Comparando as estrategias de escrita"
    $COMPOSE exec -T airflow python /opt/airflow/dags/ecommerce/benchmark.py
}

# ------------------------------------------------------------------ dispatch
case "${1:-}" in
    ""|up|start)
        bring_up
        run_demo
        summary
        ;;
    demo)   detect_compose; run_demo ;;
    verify) detect_compose; run_verify ;;
    test)   detect_compose; run_tests ;;
    load)   detect_compose; run_load ;;
    dag)    detect_compose; run_dag "${2:-}" ;;
    bench)  detect_compose; run_bench ;;
    status) detect_compose; $COMPOSE ps ;;
    logs)   detect_compose; $COMPOSE logs -f ${2:-} ;;
    down)   detect_compose; $COMPOSE down ;;
    reset)  detect_compose; $COMPOSE down -v ;;
    -h|--help|help)
        sed -n '3,15p' "$0" | sed 's/^# \{0,1\}//'
        ;;
    *)
        fail "comando desconhecido: $1   (use ./run.sh --help)"
        ;;
esac
