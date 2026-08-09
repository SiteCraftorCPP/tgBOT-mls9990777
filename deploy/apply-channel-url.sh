#!/usr/bin/env bash
# Обновить ссылку на канал на VPS (бот + сайт).
#   bash deploy/apply-channel-url.sh
set -euo pipefail

CHANNEL_URL="${1:-https://t.me/+JARHvPSqchhjZjBi}"
BOT_ENV="/var/www/tgbot-mls9990777/.env"
SITE_ENV="/opt/projects/sait-mls9990777/.env"

set_var() {
  local file="$1"
  local key="$2"
  local value="$3"
  if grep -q "^${key}=" "${file}"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "${file}"
  else
    echo "${key}=${value}" >> "${file}"
  fi
}

set_var "${BOT_ENV}" COURSE_URL "${CHANNEL_URL}"
set_var "${SITE_ENV}" COURSE_CHANNEL_URL "${CHANNEL_URL}"

systemctl restart tgbot-mls9990777
systemctl restart sait-mls9990777

echo "BOT:  $(grep '^COURSE_URL=' "${BOT_ENV}")"
echo "SITE: $(grep '^COURSE_CHANNEL_URL=' "${SITE_ENV}")"
echo "BOT:  $(systemctl is-active tgbot-mls9990777)"
echo "SITE: $(systemctl is-active sait-mls9990777)"
