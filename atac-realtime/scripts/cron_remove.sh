#!/bin/bash
#
# Remove all ATAC ingestion cron jobs.
#
set -euo pipefail

CRON_TAG="# atac-realtime"

BEFORE=$(crontab -l 2>/dev/null | grep -c "${CRON_TAG}" || true)

if [ "$BEFORE" -eq 0 ]; then
    echo "No atac-realtime cron jobs found."
    exit 0
fi

crontab -l 2>/dev/null | grep -v "${CRON_TAG}" | crontab -

echo "Removed ${BEFORE} cron job(s)."
