#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$REPO_ROOT/target/acceptance
CLUSTER_NAME=tikvd-acc
K3S_IMAGE=rancher/k3s:v1.36.2-k3s1
ARGO_VERSION=v4.1.6
ARGO_RELEASE_URL=https://github.com/argoproj/argo-workflows/releases/download/$ARGO_VERSION/quick-start-minimal.yaml
HELLO_WORLD_URL=https://raw.githubusercontent.com/argoproj/argo-workflows/$ARGO_VERSION/examples/hello-world.yaml
STORES=${STORES:-1}
case $STORES in
1)
    COMPOSE_FILE=$REPO_ROOT/dev/docker-compose.yml
    PD_ADDRESS=127.0.0.1:2379
    PD_CONTAINER=tikvd-dev-pd-1
    ;;
3)
    COMPOSE_FILE=$REPO_ROOT/dev/docker-compose.3store.yml
    PD_ADDRESS=127.0.0.1:12379
    PD_CONTAINER=tikvd-3store-pd-1
    ;;
*)
    echo "acceptance: STORES must be 1 or 3, not $STORES" >&2
    exit 2
    ;;
esac
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
    set +e
    collect_cluster_logs
    if [[ $KEEP == 1 ]]; then
        say "KEEP=1: leaving cluster $CLUSTER_NAME and tikvd running (logs in $WORK_DIR)"
        exit "$status"
    fi
    k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || say "cleanup: could not delete k3d cluster $CLUSTER_NAME"
    for port in "${TIKVD_PORTS[@]}"; do
        stop_tikvd "$port" || say "cleanup: could not stop tikvd on :$port"
    done
    remove_own_network || say "cleanup: could not remove network $CLUSTER_NETWORK"
    if [[ $STORES == 3 ]]; then
        docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || say "cleanup: could not remove the three-store cluster"
    fi
    exit "$status"
}

pd() { curl -fsS "http://$PD_ADDRESS/pd/api/v1/$1"; }

pd_ctl() { docker exec "$PD_CONTAINER" /pd-ctl -u "http://$PD_ADDRESS" "$@"; }

pd_reports_stores_up() {
    pd stores | jq -e --argjson stores "$STORES" '[.stores[] | select(.store.state_name == "Up")] | length == $stores'
}

recreate_tikv() {
    say "recreating PD and $STORES TiKV store(s) from empty ($(basename "$COMPOSE_FILE"))"
    docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1
    docker compose -f "$COMPOSE_FILE" up -d >/dev/null 2>&1
    wait_until 120 "$STORES TiKV store(s) Up in PD" pd_reports_stores_up
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
    pd regions | jq '.count'
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

region_holding() {
    pd "region/key/$(memcomparable_hex "$1")?format=hex"
}

region_id_holding() {
    region_holding "$1" | jq '.id'
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
    pd_ctl operator add split-region "$region_id" \
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

store_address() {
    pd "store/$1" | jq -r '.store.address'
}

region_leader_store() {
    pd "region/id/$1" | jq '.leader.store_id'
}

region_leader_is() { (($(region_leader_store "$1") == $2)); }

registered_on_since() {
    log_since "$1" "$2" | grep -E "registering cdc region region_id=$3 .*store_address=\"$4\"" >/dev/null
}

show_feed_log_since() {
    log_since "$1" "$2" | grep -E "cdc region error|cdc stream closed|registering cdc region|resetting the feed" |
        cut -c1-240 | sed "s/^/acceptance: :$1 /" >&2
}

leader_region_key=k/registry/

transfer_registry_leader() {
    local region region_id from to address port
    local -A lines_before
    region=$(region_holding "$leader_region_key")
    region_id=$(jq '.id' <<<"$region")
    from=$(jq '.leader.store_id' <<<"$region")
    to=$(jq --argjson from "$from" '[.peers[] | select(.store_id != $from) | .store_id][0]' <<<"$region")
    [[ $to != null ]] || fail "region $region_id has no peer outside store $from to transfer its leader to"
    address=$(store_address "$to")
    for port in "${TIKVD_PORTS[@]}"; do
        lines_before[$port]=$(log_lines "$port")
    done
    say "transferring the leader of region $region_id (holds $leader_region_key) from store $from to store $to ($address)"
    pd_ctl operator add transfer-leader "$region_id" "$to" >&2
    wait_until 60 "PD to report store $to leading region $region_id" region_leader_is "$region_id" "$to"
    say "PD reports region $region_id led by store $(region_leader_store "$region_id")"
    for port in "${TIKVD_PORTS[@]}"; do
        wait_until 60 "tikvd on :$port to register region $region_id on store $to ($address)" \
            registered_on_since "$port" "${lines_before[$port]}" "$region_id" "$address"
        show_feed_log_since "$port" "${lines_before[$port]}"
    done
}

store_container() {
    local address=$1 service
    case ${address##*:} in
    12160) service=tikv1 ;;
    12161) service=tikv2 ;;
    12162) service=tikv3 ;;
    *) fail "store address $address is not one of the three-store cluster's" ;;
    esac
    echo "tikvd-3store-$service-1"
}

leaders_on_store() {
    pd regions | jq --argjson store "$1" '[.regions[] | select(.leader.store_id == $store)] | length'
}

no_leaders_on_store() { (($(leaders_on_store "$1") == 0)); }

store_state_is() { [[ $(pd "store/$1" | jq -r '.store.state_name') == "$2" ]]; }

stop_registry_leader_store() {
    local region_id store address container port
    local -A lines_before
    region_id=$(region_id_holding "$leader_region_key")
    store=$(region_leader_store "$region_id")
    address=$(store_address "$store")
    container=$(store_container "$address")
    for port in "${TIKVD_PORTS[@]}"; do
        lines_before[$port]=$(log_lines "$port")
    done
    say "stopping store $store ($address, container $container), which leads region $region_id; it leads $(leaders_on_store "$store") regions"
    docker stop "$container" >/dev/null
    wait_until 120 "PD to move every leader off store $store" no_leaders_on_store "$store"
    say "PD reports $(leaders_on_store "$store") leaders on store $store; region $region_id is led by store $(region_leader_store "$region_id")"
    for port in "${TIKVD_PORTS[@]}"; do
        wait_until 60 "tikvd on :$port to register region $region_id away from store $store" \
            registered_on_since "$port" "${lines_before[$port]}" "$region_id" "$(store_address "$(region_leader_store "$region_id")")"
        show_feed_log_since "$port" "${lines_before[$port]}"
    done
    run_workflow
    report_feed_resets
    docker start "$container" >/dev/null
    wait_until 120 "store $store Up again in PD" store_state_is "$store" Up
    say "store $store is Up again"
}

run_store_disruption_checks() {
    local workflow
    workflow=$(submit_workflow)
    say "submitted $workflow, transferring a region leader mid-workflow"
    transfer_registry_leader
    await_workflow "$workflow"
    report_feed_resets

    stop_registry_leader_store
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

consistency_check_samples() {
    kubectl get --raw /metrics 2>/dev/null | grep '^apiserver_storage_consistency_checks_total' || true
}

report_cache_inconsistency() {
    local samples runs unsuccessful
    if grep -E "Cache consistency check (failed|error)" "$WORK_DIR/k3s.log" >&2; then
        fail "kube-apiserver reported a cache consistency check failure (above)"
    fi
    samples=$(consistency_check_samples)
    runs=$(awk '{ total += $NF } END { printf "%d", total }' <<<"$samples")
    unsuccessful=$(awk '!/status="success"/ { total += $NF } END { printf "%d", total }' <<<"$samples")
    if ((runs == 0)) && ! grep -qE "Cache consistency check (start|passed)" "$WORK_DIR/k3s.log"; then
        say "cache consistency gate SKIPPED: kube-apiserver's checker has not run yet (no apiserver_storage_consistency_checks_total samples, no checker lines in its log); it runs every 5 minutes, so raise SOAK_SECS to have it asserted"
        return 0
    fi
    if ((unsuccessful > 0)); then
        echo "$samples" >&2
        fail "kube-apiserver's cache consistency checker reported $unsuccessful unsuccessful checks (above)"
    fi
    say "cache consistency gate ASSERTED: kube-apiserver's checker ran $runs checks and none failed"
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
        if [[ $STORES == 3 ]]; then
            run_store_disruption_checks
        fi
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
