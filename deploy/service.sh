#!/usr/bin/env bash
# Runs collab-cluster-ingest as a transient systemd --user unit
# (`systemd-run`), so it gets crash-restart and `journalctl --user`
# logging without any unit file placed under ~/.config or /etc.
#
# Transient units don't survive a reboot -- if the machine restarts,
# run `./service.sh start` again (e.g. from a login script or cron).
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
unit=collab-cluster-ingest

usage() {
    echo "usage: $0 {start|stop|restart|status|logs}" >&2
    exit 1
}

start() {
    if systemctl --user is-active --quiet "$unit" 2>/dev/null; then
        echo "$unit is already running" >&2
        exit 1
    fi

    if [ ! -x "$repo_dir/.venv/bin/collab-cluster-ingest" ]; then
        echo "error: $repo_dir/.venv not found -- run 'uv sync' first" >&2
        exit 1
    fi

    # Optional per-deployment config; see deploy/.env.example. Never
    # committed -- keep secrets/local paths out of the repo.
    if [ -f "$repo_dir/deploy/.env" ]; then
        set -a
        # shellcheck disable=SC1091
        source "$repo_dir/deploy/.env"
        set +a
    fi

    local known_vars=(
        JETSTREAM_URL OUTPUT_DIR STATE_DB_PATH ALLOWED_PUBLISHER_DIDS
        TARGET_STAC_COLLECTION WORKER_CONCURRENCY QUEUE_MAXSIZE
        HTTP_TIMEOUT_SECONDS
    )
    local setenv_args=()
    for v in "${known_vars[@]}"; do
        if [ -n "${!v:-}" ]; then
            setenv_args+=(--setenv="$v=${!v}")
        fi
    done

    systemd-run --user \
        --unit="$unit" \
        --description="collab-cluster-ingest (matadisco -> torrent pipeline)" \
        --working-directory="$repo_dir" \
        -p "Restart=on-failure" \
        -p "RestartSec=5" \
        "${setenv_args[@]}" \
        "$repo_dir/.venv/bin/collab-cluster-ingest"
}

case "${1:-}" in
    start) start ;;
    stop) systemctl --user stop "$unit" ;;
    restart) systemctl --user stop "$unit" 2>/dev/null || true; start ;;
    status) systemctl --user status "$unit" ;;
    logs) journalctl --user -u "$unit" -f ;;
    *) usage ;;
esac
