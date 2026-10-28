#!/usr/bin/env bash
# Connects mongosh to a Firestore Enterprise (MongoDB Compatibility) database
# using a freshly minted, URL-encoded 60-minute Google Cloud OAuth2 token.
#
# Usage:
#   ./scripts/mongo_connect.sh --project <PROJECT_ID> --database <DATABASE_ID> [extra mongosh args...]
#
# Examples:
#   ./scripts/mongo_connect.sh --project my-gcp-project --database voicebot-demo
#   ./scripts/mongo_connect.sh --project my-gcp-project --database voicebot-demo --eval 'db.calls.countDocuments()'
#   ./scripts/mongo_connect.sh --project my-gcp-project --database voicebot-demo --file scripts/live_validation_queries.js

set -euo pipefail

PROJECT="${PROJECT_ID:-}"
DB="${DB_ID:-voicebot-demo}"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project|-p)
      PROJECT="$2"
      shift 2
      ;;
    --database|-d)
      DB="$2"
      shift 2
      ;;
    --help|-h)
      sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "${PROJECT}" ]]; then
  PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
  if [[ "${PROJECT}" == "(unset)" ]]; then
    PROJECT=""
  fi
fi

if [[ -z "${PROJECT}" ]]; then
  echo "ERROR: Specify --project <YOUR_GCP_PROJECT_ID> or export PROJECT_ID." >&2
  exit 1
fi

DB_UID="$(gcloud firestore databases describe --database="${DB}" --project="${PROJECT}" --format='value(uid)')"
LOC="$(gcloud firestore databases describe --database="${DB}" --project="${PROJECT}" --format='value(locationId)')"

if [[ -z "${DB_UID}" || -z "${LOC}" ]]; then
  echo "ERROR: Could not resolve Firestore Enterprise database '${DB}' in project '${PROJECT}'." >&2
  exit 1
fi

TOKEN="$(gcloud auth print-access-token | python3 -c "import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read().strip(), safe=''))")"
DB_PATH="${DB}"
if [[ "${DB}" == "(default)" ]]; then
  DB_PATH="_"
fi

URI="mongodb://access_token:${TOKEN}@${DB_UID}.${LOC}.firestore.goog:443/${DB_PATH}?loadBalanced=true&tls=true&retryWrites=false&authMechanism=PLAIN&authSource=%24external"

echo "Connecting mongosh to database '${DB}' (${LOC})..."
exec mongosh "${URI}" "${EXTRA_ARGS[@]}"
