#!/usr/bin/env bash
set -euo pipefail

SETUP_SYSTEMD=0

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
DASHBOARD_SERVICE="${DASHBOARD_SERVICE:-}"
PYTHON="${PYTHON:-python3}"
ACTIVITY_BUILD="${ACTIVITY_BUILD:-1}"
ACTIVITY_DIR="${ACTIVITY_DIR:-DCActivity}"
ACTIVITY_PM2_NAME="${ACTIVITY_PM2_NAME:-716Stonks-activity}"
ACTIVITY_PORT="${ACTIVITY_PORT:-8083}"
ACTIVITY_API_BASE="${ACTIVITY_API_BASE:-${DASHBOARD_PUBLIC_BASE_URL:-}}"
ACTIVITY_DISCORD_CLIENT_ID="${ACTIVITY_DISCORD_CLIENT_ID:-}"
ACTIVITY_DISCORD_REDIRECT_URI="${ACTIVITY_DISCORD_REDIRECT_URI:-}"

RSYNC_EXCLUDES=(
  ".git"
  "__pycache__"
  ".venv"
  "node_modules"
  "data"
  ".vscode"
  "DCActivity/dist"
  "DCActivity/.vite"
  "*.code-workspace"
)

ssh -p "${VPS_PORT}" "${VPS_USER}@${VPS_HOST}" "test -d '${VPS_DIR}' || mkdir -p '${VPS_DIR}'"

rsync -az --delete -e "ssh -p ${VPS_PORT}" \
  $(printf -- "--exclude=%s " "${RSYNC_EXCLUDES[@]}") \
  ./ "${VPS_USER}@${VPS_HOST}:${VPS_DIR}/"

ssh -p "${VPS_PORT}" "${VPS_USER}@${VPS_HOST}" "bash -se" <<SSH
  set -euo pipefail
  if [ "\$(id -u)" -eq 0 ]; then
    SUDO=""
  else
    if sudo -n true >/dev/null 2>&1; then
      SUDO="sudo -n"
    else
      echo "Passwordless sudo is required for deploy. Configure sudoers or use root for bootstrap."
      exit 1
    fi
  fi
  detect_venv_pkg() {
    PY_VER="\$(${PYTHON} -V 2>&1 | cut -d' ' -f2 | cut -d. -f1,2)"
    echo "python\${PY_VER}-venv"
  }

  cd "${VPS_DIR}"
  if [ ! -x .venv/bin/python ]; then
    if ! ${PYTHON} -m venv .venv; then
      if command -v apt-get >/dev/null 2>&1; then
        VENV_PKG="\$(detect_venv_pkg)"
        \$SUDO apt-get update -y || true
        \$SUDO apt-get install -y "\$VENV_PKG"
        ${PYTHON} -m venv .venv
      else
        echo "Failed to create .venv with ${PYTHON}. Install venv support and re-run deploy."
        exit 1
      fi
    fi
  fi

  if [ ! -x .venv/bin/python ]; then
    echo "Project venv is missing .venv/bin/python after setup."
    exit 1
  fi

  if ! .venv/bin/python -m pip --version >/dev/null 2>&1; then
    if ! .venv/bin/python -m ensurepip --upgrade >/dev/null 2>&1; then
      if command -v apt-get >/dev/null 2>&1; then
        VENV_PKG="\$(detect_venv_pkg)"
        \$SUDO apt-get update -y || true
        \$SUDO apt-get install -y "\$VENV_PKG" python3-pip
        rm -rf .venv
        ${PYTHON} -m venv .venv
        .venv/bin/python -m ensurepip --upgrade >/dev/null 2>&1 || true
      else
        echo "Project venv is missing pip and could not be repaired automatically."
        exit 1
      fi
    fi
  fi

  if ! .venv/bin/python -m pip --version >/dev/null 2>&1; then
    echo "Project venv is missing pip after setup."
    exit 1
  fi

  .venv/bin/python -m pip install --upgrade pip
  .venv/bin/python -m pip install -r requirements.txt

  if [ "${ACTIVITY_BUILD}" = "1" ]; then
    if [ -d "${VPS_DIR}/${ACTIVITY_DIR}" ] && [ -f "${VPS_DIR}/${ACTIVITY_DIR}/package.json" ]; then
      if command -v npm >/dev/null 2>&1; then
        cd "${VPS_DIR}/${ACTIVITY_DIR}"
        npm install
        if [ -n "${ACTIVITY_API_BASE}" ]; then
          VITE_API_BASE="${ACTIVITY_API_BASE}" \
          VITE_DISCORD_CLIENT_ID="${ACTIVITY_DISCORD_CLIENT_ID}" \
          VITE_DISCORD_REDIRECT_URI="${ACTIVITY_DISCORD_REDIRECT_URI}" \
          npm run build
        else
          echo "Warning: ACTIVITY_API_BASE is empty; Activity /api calls will hit static host."
          VITE_DISCORD_CLIENT_ID="${ACTIVITY_DISCORD_CLIENT_ID}" \
          VITE_DISCORD_REDIRECT_URI="${ACTIVITY_DISCORD_REDIRECT_URI}" \
          npm run build
        fi
        cd "${VPS_DIR}"
        if command -v pm2 >/dev/null 2>&1; then
          if pm2 describe "${ACTIVITY_PM2_NAME}" >/dev/null 2>&1; then
            pm2 restart "${ACTIVITY_PM2_NAME}" --update-env
          else
            pm2 serve "${VPS_DIR}/${ACTIVITY_DIR}/dist" "${ACTIVITY_PORT}" --name "${ACTIVITY_PM2_NAME}"
          fi
          pm2 save || true
        else
          echo "Skipping activity PM2 step: pm2 not found."
        fi
      else
        echo "Skipping activity build: npm not found."
      fi
    else
      echo "Skipping activity build: ${ACTIVITY_DIR} not found or missing package.json."
    fi
  fi

  UNIT_FILE="/etc/systemd/system/${APP_NAME}.service"
  if [ "${SETUP_SYSTEMD}" = "1" ]; then
    TMP_UNIT="${VPS_DIR}/.${APP_NAME}.service.tmp"
    cat > "\${TMP_UNIT}" <<UNIT
[Unit]
Description=${APP_NAME}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${VPS_USER}
WorkingDirectory=${VPS_DIR}
ExecStart=${VPS_DIR}/.venv/bin/python -m stockbot
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
UNIT

    \$SUDO install -m 644 "\${TMP_UNIT}" "\$UNIT_FILE"
    rm -f "\${TMP_UNIT}"
    \$SUDO systemctl daemon-reload
    \$SUDO systemctl enable "${APP_NAME}.service"
  elif ! \$SUDO systemctl cat "${APP_NAME}.service" >/dev/null 2>&1; then
    echo "Missing ${APP_NAME}.service. Run deploy once with SETUP_SYSTEMD=1."
    exit 1
  fi

  \$SUDO systemctl restart "${APP_NAME}.service"
  \$SUDO systemctl --no-pager --full status "${APP_NAME}.service"

  if [ -n "${DASHBOARD_SERVICE}" ]; then
    if \$SUDO systemctl cat "${DASHBOARD_SERVICE}" >/dev/null 2>&1; then
      \$SUDO systemctl restart "${DASHBOARD_SERVICE}"
      \$SUDO systemctl --no-pager --full status "${DASHBOARD_SERVICE}"
    else
      echo "Skipping dashboard restart: ${DASHBOARD_SERVICE} not found."
    fi
  fi
SSH
