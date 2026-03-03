#!/bin/sh
# entrypoint.sh — Fix directory ownership then drop to PUID:PGID
set -e

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

mkdir -p /app/data
chown -R "$PUID:$PGID" /app/data

exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups \
    /usr/local/bin/wesense-iroh-sidecar "$@"
