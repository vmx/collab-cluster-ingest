#!/usr/bin/env bash
# Manages collab-cluster-torrentizer as a systemd --user unit, two ways:
#
#   start/stop/restart/status/logs -- a *transient* unit (`systemd-run`),
#     with crash-restart and `journalctl --user` logging but no unit file
#     placed under ~/.config or /etc. Doesn't survive a reboot -- run
#     `./service.sh start` again after one (e.g. from a login script or
#     cron).
#
#   install/uninstall -- a *persistent* unit that does survive reboots.
#     The real unit file is generated from
#     collab-cluster-torrentizer.service.template and kept in this repo
#     (deploy/collab-cluster-torrentizer.service, gitignored -- it embeds this
#     machine's absolute repo path). `systemctl --user link` only adds a
#     symlink under ~/.config/systemd/user pointing back at it, so
#     nothing but that symlink is written outside the project directory.
#
# Both modes manage the same unit name, so only one can be active at a
# time -- stop/uninstall one before using the other.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
unit=collab-cluster-torrentizer
template="$repo_dir/deploy/collab-cluster-torrentizer.service.template"
unit_file="$repo_dir/deploy/collab-cluster-torrentizer.service"

usage() {
    echo "usage: $0 {start|stop|restart|status|logs|install|uninstall}" >&2
    exit 1
}

require_venv() {
    if [ ! -x "$repo_dir/.venv/bin/collab-cluster-torrentizer" ]; then
        echo "error: $repo_dir/.venv not found -- run 'uv sync' first" >&2
        exit 1
    fi
}

start() {
    if systemctl --user is-active --quiet "$unit" 2>/dev/null; then
        echo "$unit is already running" >&2
        exit 1
    fi
    if systemctl --user is-enabled --quiet "$unit" 2>/dev/null; then
        echo "$unit is installed as a persistent unit -- use 'systemctl --user start $unit' or '$0 uninstall' first" >&2
        exit 1
    fi

    require_venv

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
        --description="collab-cluster-torrentizer (matadisco -> torrent pipeline)" \
        --working-directory="$repo_dir" \
        -p "Restart=on-failure" \
        -p "RestartSec=5" \
        "${setenv_args[@]}" \
        "$repo_dir/.venv/bin/collab-cluster-torrentizer"
}

install_unit() {
    if systemctl --user is-active --quiet "$unit" 2>/dev/null; then
        echo "$unit is already running (as the transient unit -- run '$0 stop' first)" >&2
        exit 1
    fi

    require_venv

    sed "s|__REPO_DIR__|$repo_dir|g" "$template" > "$unit_file"

    systemctl --user link "$unit_file"
    systemctl --user daemon-reload
    systemctl --user enable --now "$unit"

    echo "installed and started -- see: systemctl --user status $unit"
}

uninstall_unit() {
    # `disable` removes both the enablement symlink and the one `link`
    # created, leaving only the generated file in this repo.
    systemctl --user disable --now "$unit" 2>/dev/null || true
    systemctl --user daemon-reload
    rm -f "$unit_file"
    echo "uninstalled"
}

case "${1:-}" in
    start) start ;;
    stop) systemctl --user stop "$unit" ;;
    restart) systemctl --user stop "$unit" 2>/dev/null || true; start ;;
    status) systemctl --user status "$unit" ;;
    logs) journalctl --user -u "$unit" -f ;;
    install) install_unit ;;
    uninstall) uninstall_unit ;;
    *) usage ;;
esac
