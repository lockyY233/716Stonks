#!/usr/bin/env bash
set -euo pipefail

if [ -f ".env.deploy" ]; then
  set -a
  # shellcheck disable=SC1091
  source ".env.deploy"
  set +a
fi

VPS_USER="${VPS_USER:-youruser}"
VPS_HOST="${VPS_HOST:-your.vps.ip}"
VPS_PORT="${VPS_PORT:-22}"
VPS_DIR="${VPS_DIR:-/home/${VPS_USER}/apps/716Stonks}"
APP_NAME="${APP_NAME:-716Stonks}"
PYTHON="${PYTHON:-python3}"

RSYNC_EXCLUDES=(
  ".git"
  "__pycache__"
  ".venv"
  "TOKEN"
  "data"
  ".vscode"
  "*.code-workspace"
)

ssh -p "${VPS_PORT}" "${VPS_USER}@${VPS_HOST}" "test -d '${VPS_DIR}' || mkdir -p '${VPS_DIR}'"

rsync -az --delete -e "ssh -p ${VPS_PORT}" \
  $(printf -- "--exclude=%s " "${RSYNC_EXCLUDES[@]}") \
  ./ "${VPS_USER}@${VPS_HOST}:${VPS_DIR}/"

ssh -p "${VPS_PORT}" "${VPS_USER}@${VPS_HOST}" <<SSH
  set -euo pipefail
  cd "${VPS_DIR}"
  ${PYTHON} -m venv .venv || true
  source .venv/bin/activate
  pip install -r requirements.txt
  pm2 start ecosystem.config.js || pm2 reload ecosystem.config.js
  pm2 save
SSH
