#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$REPO_ROOT/target/acceptance
CLUSTER_NAME=tikvd-acc
K3S_IMAGE=rancher/k3s:v1.36.2-k3s1
ARGO_VERSION=v4.1.6
ARGO_RELEASE_URL=https://github.com/argoproj/argo-workflows/releases/download/$ARGO_VERSION/quick-start-minimal.yaml
HELLO_WORLD_URL=https://raw.githubusercontent.com/argoproj/argo-workflows/$ARGO_VERSION/examples/hello-world.yaml
PD_ADDRESS=127.0.0.1:2379
TIKVD_PORTS=(2479 2480)
TIKVD_LOG_FILTER=info,tikvd=debug
NODE_CONTAINER=k3d-$CLUSTER_NAME-server-0
CLUSTER_NETWORK=$CLUSTER_NAME-net
NETWORK_LABEL=tikvd.acceptance=$CLUSTER_NAME
CLUSTER_SUBNET=${TIKVD_ACC_SUBNET:-172.29.42.0/24}
HOST_GATEWAY=$(python3 -I -c 'import ipaddress, sys; print(next(ipaddress.ip_network(sys.argv[1]).hosts()))' "$CLUSTER_SUBNET")
TIKVD_BINARY=$REPO_ROOT/target/release/tikvd
WORKFLOW_TIMEOUT_SECS=300
SOAK_SECS=${SOAK_SECS:-0}
DISRUPT=${DISRUPT:-0}
KEEP=${KEEP:-0}

export KUBECONFIG=$WORK_DIR/kubeconfig

say() { echo "acceptance: $*" >&2; }
fail() {
    say "FAILED: $*"
    exit 1
}

tikvd_log() { echo "$WORK_DIR/tikvd-$1.log"; }
tikvd_pid_file() { echo "$WORK_DIR/tikvd-$1.pid"; }

wait_until() {
    local timeout_secs=$1 description=$2
    shift 2
    local deadline=$((SECONDS + timeout_secs))
    until "$@" >/dev/null 2>&1; do
        ((SECONDS < deadline)) || fail "timed out after ${timeout_secs}s waiting for $description"
        sleep 2
    done
}

collect_cluster_logs() {
    docker logs "$NODE_CONTAINER" >"$WORK_DIR/k3s.log" 2>&1 || true
    kubectl -n argo logs deployment/workflow-controller >"$WORK_DIR/workflow-controller.log" 2>&1 || true
}

cleanup() {
    local status=$?
    collect_cluster_logs
    if [[ $KEEP == 1 ]]; then
        say "KEEP=1: leaving cluster $CLUSTER_NAME and tikvd running (logs in $WORK_DIR)"
        exit "$status"
    fi
    k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
    for port in "${TIKVD_PORTS[@]}"; do
        stop_tikvd "$port"
    done
    remove_own_network
    exit "$status"
}

pd_reports_store_up() {
    curl -fsS "http://$PD_ADDRESS/pd/api/v1/stores" | jq -e '.stores[0].store.state_name == "Up"'
}

recreate_tikv() {
    say "recreating PD and TiKV from empty"
    docker compose -f "$REPO_ROOT/dev/docker-compose.yml" down >/dev/null 2>&1
    docker compose -f "$REPO_ROOT/dev/docker-compose.yml" up -d >/dev/null 2>&1
    wait_until 120 "TiKV store Up in PD" pd_reports_store_up
}

build_tikvd() {
    say "building tikvd (release)"
    cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml" >/dev/null 2>&1 || fail "cargo build"
}

tikvd_feed_started() { tail -n "+$(($2 + 1))" "$(tikvd_log "$1")" | grep "cdc feed starting" >/dev/null; }

start_tikvd() {
    local port=$1 lines_before=0
    [[ -f $(tikvd_log "$port") ]] && lines_before=$(wc -l <"$(tikvd_log "$port")")
    NO_COLOR=1 RUST_LOG=$TIKVD_LOG_FILTER "$TIKVD_BINARY" \
        --listen "$HOST_GATEWAY:$port" --pd "$PD_ADDRESS" >>"$(tikvd_log "$port")" 2>&1 &
    echo $! >"$(tikvd_pid_file "$port")"
    echo "$port" >>"$WORK_DIR/tikvd-starts"
    wait_until 60 "tikvd on :$port" tikvd_feed_started "$port" "$lines_before"
    say "tikvd on :$port started (pid $(cat "$(tikvd_pid_file "$port")"))"
}

is_own_tikvd() {
    local exe
    exe=$(readlink "/proc/$1/exe" 2>/dev/null) || return 1
    [[ $exe == "$TIKVD_BINARY" || $exe == "$TIKVD_BINARY (deleted)" ]]
}

stop_tikvd() {
    local pid_file pid
    pid_file=$(tikvd_pid_file "$1")
    [[ -f $pid_file ]] || return 0
    pid=$(cat "$pid_file")
    if is_own_tikvd "$pid"; then
        kill "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_file"
}

kill_tikvd() {
    local pid_file
    pid_file=$(tikvd_pid_file "$1")
    say "killing tikvd on :$1 (pid $(cat "$pid_file"))"
    kill -9 "$(cat "$pid_file")"
    rm -f "$pid_file"
}

datastore_endpoint() {
    local host=$1 endpoints=()
    for port in "${TIKVD_PORTS[@]}"; do
        endpoints+=("http://$host:$port")
    done
    local IFS=,
    echo "${endpoints[*]}"
}

network_is_own() {
    [[ $(docker network inspect "$CLUSTER_NETWORK" --format '{{index .Labels "tikvd.acceptance"}}' 2>/dev/null) == "$CLUSTER_NAME" ]]
}

remove_own_network() {
    if network_is_own; then
        docker network rm "$CLUSTER_NETWORK" >/dev/null
    fi
}

subnets_overlap() {
    python3 -I -c 'import ipaddress, sys; sys.exit(not ipaddress.ip_network(sys.argv[1]).overlaps(ipaddress.ip_network(sys.argv[2])))' "$1" "$2"
}

network_using_subnet() {
    local name subnet
    for name in $(docker network ls --format '{{.Name}}'); do
        for subnet in $(docker network inspect "$name" --format '{{range .IPAM.Config}}{{.Subnet}} {{end}}'); do
            if subnets_overlap "$subnet" "$CLUSTER_SUBNET" 2>/dev/null; then
                echo "$name"
                return 0
            fi
        done
    done
    return 1
}

create_network() {
    local taken_by
    if docker network inspect "$CLUSTER_NETWORK" >/dev/null 2>&1 && ! network_is_own; then
        fail "a network named $CLUSTER_NETWORK exists and was not created by this script"
    fi
    remove_own_network
    if taken_by=$(network_using_subnet); then
        fail "subnet $CLUSTER_SUBNET is already used by docker network $taken_by; set TIKVD_ACC_SUBNET to a free subnet"
    fi
    docker network create --driver bridge --subnet "$CLUSTER_SUBNET" --gateway "$HOST_GATEWAY" \
        --label "$NETWORK_LABEL" "$CLUSTER_NETWORK" >/dev/null
    say "created network $CLUSTER_NETWORK ($CLUSTER_SUBNET); tikvd listens on $HOST_GATEWAY only"
}

create_cluster() {
    say "creating k3d cluster $CLUSTER_NAME ($K3S_IMAGE) on $(datastore_endpoint "$HOST_GATEWAY")"
    k3d cluster create "$CLUSTER_NAME" \
        --image "$K3S_IMAGE" --servers 1 --agents 0 --no-lb --network "$CLUSTER_NETWORK" \
        --k3s-arg "--datastore-endpoint=$(datastore_endpoint "$HOST_GATEWAY")@server:0" \
        --kubeconfig-update-default=false --kubeconfig-switch-context=false \
        --timeout 300s >"$WORK_DIR/k3d-create.log" 2>&1 || fail "k3d cluster create (see $WORK_DIR/k3d-create.log)"
    k3d kubeconfig get "$CLUSTER_NAME" >"$KUBECONFIG"
}

node_is_ready() {
    kubectl get nodes -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' | grep -q True
}

fetch_argo_manifests() {
    curl -fsSL "$ARGO_RELEASE_URL" -o "$WORK_DIR/quick-start-minimal.yaml" || fail "fetch $ARGO_RELEASE_URL"
    curl -fsSL "$HELLO_WORLD_URL" -o "$WORK_DIR/hello-world.yaml" || fail "fetch $HELLO_WORLD_URL"
}

install_argo() {
    say "installing Argo Workflows $ARGO_VERSION"
    kubectl create namespace argo >/dev/null
    kubectl apply --server-side -n argo -f "$WORK_DIR/quick-start-minimal.yaml" >/dev/null
    kubectl -n argo wait deployment/workflow-controller --for=condition=Available --timeout=300s >/dev/null ||
        fail "workflow-controller did not become available"
}

submit_workflow() {
    kubectl -n argo create -f "$WORK_DIR/hello-world.yaml" -o name
}

workflow_phase() {
    kubectl -n argo get "$1" -o jsonpath='{.status.phase}' 2>/dev/null || true
}

await_workflow() {
    local workflow=$1 phase deadline=$((SECONDS + WORKFLOW_TIMEOUT_SECS))
    while ((SECONDS < deadline)); do
        phase=$(workflow_phase "$workflow")
        case $phase in
        Succeeded)
            say "$workflow Succeeded"
            return 0
            ;;
        Failed | Error) fail "$workflow ended $phase" ;;
        esac
        sleep 2
    done
    fail "$workflow did not finish within ${WORKFLOW_TIMEOUT_SECS}s (phase '${phase:-}')"
}

run_workflow() {
    local workflow
    workflow=$(submit_workflow)
    say "submitted $workflow"
    await_workflow "$workflow"
}

region_count() {
    curl -fsS "http://$PD_ADDRESS/pd/api/v1/regions" | jq '.count'
}

region_count_above() { (($(region_count) > $1)); }

memcomparable_hex() {
    python3 -I -c '
import sys
raw = sys.argv[1].encode()
out = b""
for i in range(0, len(raw) + 1, 8):
    group = raw[i:i + 8]
    pad = 8 - len(group)
    out += group + b"\0" * pad + bytes([0xFF - pad])
print(out.hex().upper())' "$1"
}

region_id_holding() {
    curl -fsS "http://$PD_ADDRESS/pd/api/v1/region/key/$(memcomparable_hex "$1")?format=hex" | jq '.id'
}

log_lines() { wc -l <"$(tikvd_log "$1")"; }

log_since() { tail -n "+$(($2 + 1))" "$(tikvd_log "$1")"; }

registered_since() { log_since "$1" "$2" | grep "registering cdc region region_id=$3 " >/dev/null; }

split_registry_region() {
    local split_key=k/registry/pods/ left_key=k/ region_id before port left_id right_id
    local -A lines_before
    region_id=$(region_id_holding "$split_key")
    before=$(region_count)
    for port in "${TIKVD_PORTS[@]}"; do
        lines_before[$port]=$(log_lines "$port")
    done
    say "splitting region $region_id at $split_key ($before regions)"
    docker exec tikvd-dev-pd-1 /pd-ctl -u "http://$PD_ADDRESS" operator add split-region "$region_id" \
        --policy=usekey --keys="$(memcomparable_hex "$split_key")" >&2
    wait_until 60 "PD to report the split" region_count_above "$before"
    left_id=$(region_id_holding "$left_key")
    right_id=$(region_id_holding "$split_key")
    say "PD reports $(region_count) regions after the split: $left_id below $split_key, $right_id from it"
    for port in "${TIKVD_PORTS[@]}"; do
        wait_until 30 "tikvd on :$port to register region $left_id" registered_since "$port" "${lines_before[$port]}" "$left_id"
        wait_until 30 "tikvd on :$port to register region $right_id" registered_since "$port" "${lines_before[$port]}" "$right_id"
        log_since "$port" "${lines_before[$port]}" | grep -E "cdc region error|registering cdc region" |
            cut -c1-200 | sed "s/^/acceptance: :$port /" >&2
    done
}

run_disruption_checks() {
    local workflow
    workflow=$(submit_workflow)
    say "submitted $workflow, killing tikvd on :${TIKVD_PORTS[0]} mid-workflow"
    sleep 3
    kill_tikvd "${TIKVD_PORTS[0]}"
    await_workflow "$workflow"

    start_tikvd "${TIKVD_PORTS[0]}"
    sleep 5
    kill_tikvd "${TIKVD_PORTS[1]}"
    run_workflow
    start_tikvd "${TIKVD_PORTS[1]}"

    workflow=$(submit_workflow)
    say "submitted $workflow, splitting the k/ region mid-workflow"
    split_registry_region
    await_workflow "$workflow"
}

report_unimplemented() {
    local found=0
    for port in "${TIKVD_PORTS[@]}"; do
        if grep UNIMPLEMENTED "$(tikvd_log "$port")" >&2; then
            found=1
        fi
    done
    ((found == 0)) || fail "tikvd logged UNIMPLEMENTED requests (above)"
}

report_feed_resets() {
    local port starts feed_starts
    for port in "${TIKVD_PORTS[@]}"; do
        if grep "resetting the feed" "$(tikvd_log "$port")" >&2; then
            fail "tikvd on :$port reset its cdc feed (above)"
        fi
        starts=$(grep -cx "$port" "$WORK_DIR/tikvd-starts")
        feed_starts=$(grep -c "cdc feed starting" "$(tikvd_log "$port")")
        ((feed_starts == starts)) || fail "tikvd on :$port started its cdc feed $feed_starts times in $starts process starts"
    done
}

report_cache_inconsistency() {
    if grep "Cache consistency check" "$WORK_DIR/k3s.log" >&2; then
        fail "kube-apiserver reported a cache consistency check failure (above)"
    fi
}

main() {
    trap cleanup EXIT
    k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
    for port in "${TIKVD_PORTS[@]}"; do
        stop_tikvd "$port"
    done
    rm -rf "$WORK_DIR"
    mkdir -p "$WORK_DIR"
    fetch_argo_manifests
    build_tikvd
    recreate_tikv
    create_network
    for port in "${TIKVD_PORTS[@]}"; do
        start_tikvd "$port"
    done
    create_cluster
    wait_until 300 "node Ready" node_is_ready
    install_argo
    run_workflow
    if [[ $DISRUPT == 1 ]]; then
        run_disruption_checks
    fi
    if ((SOAK_SECS > 0)); then
        say "soaking for ${SOAK_SECS}s"
        sleep "$SOAK_SECS"
        run_workflow
    fi
    collect_cluster_logs
    report_cache_inconsistency
    report_feed_resets
    report_unimplemented
    echo "workflow Succeeded"
}

main "$@"
