#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/var/www/tgbot-mls9990777"
REPO_URL="https://github.com/SiteCraftorCPP/tgBOT-mls9990777.git"
SERVICE_NAME="tgbot-mls9990777"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Запускайте от root: sudo bash deploy/install-vps.sh"
  exit 1
fi

if [[ -d "${APP_DIR}/.git" ]]; then
  echo "Проект уже установлен в ${APP_DIR}. Для обновления: bash deploy/update-vps.sh"
  exit 1
fi

mkdir -p "${APP_DIR}"
git clone "${REPO_URL}" "${APP_DIR}"
cd "${APP_DIR}"

if ! command -v python3 >/dev/null 2>&1; then
  apt-get update
  apt-get install -y python3 python3-venv python3-pip git
fi

python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

mkdir -p data

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo ""
  echo "Создан ${APP_DIR}/.env — заполните BOT_TOKEN и остальные переменные."
  echo "На VPS обычно TELEGRAM_PROXY не нужен — оставьте пустым."
fi

cp deploy/tgbot-mls9990777.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"

echo ""
echo "Установка завершена."
echo "1) Отредактируйте ${APP_DIR}/.env"
echo "2) systemctl start ${SERVICE_NAME}"
echo "3) systemctl status ${SERVICE_NAME}"
