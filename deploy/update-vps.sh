#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/var/www/tgbot-mls9990777"
SERVICE_NAME="tgbot-mls9990777"

cd "${APP_DIR}"
git pull --ff-only
.venv/bin/pip install -r requirements.txt
systemctl restart "${SERVICE_NAME}"
systemctl --no-pager status "${SERVICE_NAME}"
