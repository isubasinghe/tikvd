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
CLUSTER_NETWORK=k3d-$CLUSTER_NAME
CLUSTER_SUBNET=172.29.42.0/24
HOST_GATEWAY=172.29.42.1
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

tikvd_feed_started() { tail -n "+$(($2 + 1))" "$(tikvd_log "$1")" | grep -q "cdc feed starting"; }

start_tikvd() {
    local port=$1 lines_before=0
    [[ -f $(tikvd_log "$port") ]] && lines_before=$(wc -l <"$(tikvd_log "$port")")
    NO_COLOR=1 RUST_LOG=$TIKVD_LOG_FILTER "$REPO_ROOT/target/release/tikvd" \
        --listen "0.0.0.0:$port" --pd "$PD_ADDRESS" >>"$(tikvd_log "$port")" 2>&1 &
    echo $! >"$(tikvd_pid_file "$port")"
    wait_until 60 "tikvd on :$port" tikvd_feed_started "$port" "$lines_before"
    say "tikvd on :$port started (pid $(cat "$(tikvd_pid_file "$port")"))"
}

stop_tikvd() {
    local pid_file
    pid_file=$(tikvd_pid_file "$1")
    [[ -f $pid_file ]] || return 0
    kill "$(cat "$pid_file")" 2>/dev/null || true
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

cluster_gateway() {
    docker network inspect "$CLUSTER_NETWORK" --format '{{range .IPAM.Config}}{{.Gateway}}{{end}}'
}

create_cluster() {
    say "creating k3d cluster $CLUSTER_NAME ($K3S_IMAGE) on $(datastore_endpoint "$HOST_GATEWAY")"
    k3d cluster create "$CLUSTER_NAME" \
        --image "$K3S_IMAGE" --servers 1 --agents 0 --no-lb --subnet "$CLUSTER_SUBNET" \
        --k3s-arg "--datastore-endpoint=$(datastore_endpoint "$HOST_GATEWAY")@server:0" \
        --kubeconfig-update-default=false --kubeconfig-switch-context=false \
        --timeout 300s >"$WORK_DIR/k3d-create.log" 2>&1 || fail "k3d cluster create (see $WORK_DIR/k3d-create.log)"
    k3d kubeconfig get "$CLUSTER_NAME" >"$KUBECONFIG"
    [[ $(cluster_gateway) == "$HOST_GATEWAY" ]] || fail "$CLUSTER_NETWORK gateway is $(cluster_gateway), not $HOST_GATEWAY"
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

split_registry_region() {
    local split_key=k/registry/pods/ region_id before
    region_id=$(curl -fsS "http://$PD_ADDRESS/pd/api/v1/region/key/$(memcomparable_hex "$split_key")?format=hex" | jq '.id')
    before=$(region_count)
    say "splitting region $region_id at $split_key ($before regions)"
    docker exec tikvd-dev-pd-1 /pd-ctl -u "http://$PD_ADDRESS" operator add split-region "$region_id" \
        --policy=usekey --keys="$(memcomparable_hex "$split_key")" >&2
    wait_until 60 "PD to report the split" region_count_above "$before"
    say "PD reports $(region_count) regions after the split"
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
    report_unimplemented
    echo "workflow Succeeded"
}

main "$@"
