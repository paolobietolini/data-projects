#!/bin/bash
#
# Download the latest ATAC static GTFS feed if it has changed.
# Archives the previous version with a date stamp.
#
set -euo pipefail

STATIC_DIR="$(dirname "$0")/../static"
cd "$STATIC_DIR"

ZIP_URL="https://romamobilita.it/sites/default/files/rome_static_gtfs.zip"
MD5_URL="${ZIP_URL}.md5"

NEW_MD5=$(curl -sf "$MD5_URL" | awk '{print $1}')
OLD_MD5=$(md5sum rome_static_gtfs.zip 2>/dev/null | awk '{print $1}' || echo "none")

if [ "$NEW_MD5" = "$OLD_MD5" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') Static GTFS unchanged (md5: ${OLD_MD5:0:12}...)"
    exit 0
fi

TODAY=$(date +%Y-%m-%d)
echo "$(date '+%Y-%m-%d %H:%M:%S') Static GTFS changed, downloading..."

# Archive current version
if [ -f rome_static_gtfs.zip ]; then
    mkdir -p archive
    cp rome_static_gtfs.zip "archive/rome_static_gtfs_${TODAY}.zip"
    echo "  Archived previous version to archive/rome_static_gtfs_${TODAY}.zip"
fi

# Download new version
curl -sf -o rome_static_gtfs.zip "$ZIP_URL"

# Extract (overwrite the individual txt files)
unzip -o rome_static_gtfs.zip -x "__MACOSX/*" "*.DS_Store" 2>/dev/null || true

echo "$(date '+%Y-%m-%d %H:%M:%S') Done. Files updated:"
ls -lh *.txt 2>/dev/null | awk '{print "  " $NF " (" $5 ")"}'
