#!/bin/bash
#
# Install cron jobs for ATAC ingestion and static GTFS refresh.
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python"
INGEST="${PROJECT_DIR}/scripts/ingest_once.py"
REFRESH="${PROJECT_DIR}/scripts/refresh_static.sh"
LOG_DIR="${PROJECT_DIR}/logs"

mkdir -p "$LOG_DIR"

# Build cron entries (tagged so we can find them later)
CRON_TAG="# atac-realtime"
CRON_INGEST="* * * * * ${PYTHON} ${INGEST} >> ${LOG_DIR}/ingest.log 2>&1 ${CRON_TAG}"
CRON_REFRESH="0 4 * * * ${REFRESH} >> ${LOG_DIR}/static_refresh.log 2>&1 ${CRON_TAG}"

# Remove any existing atac-realtime entries, then add new ones
({ crontab -l 2>/dev/null || true; } | grep -v "${CRON_TAG}" || true; echo "${CRON_INGEST}"; echo "${CRON_REFRESH}") | crontab -

echo "Cron jobs installed:"
echo "  [every minute]  ingest GTFS-RT feeds → ${LOG_DIR}/ingest.log"
echo "  [daily 4am]     refresh static GTFS  → ${LOG_DIR}/static_refresh.log"
echo ""
crontab -l | grep "${CRON_TAG}"
