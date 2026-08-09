#!/usr/bin/env bash
# Запускать НА VPS в папке проекта:
#   cd /var/www/tgbot-mls9990777
#   bash deploy/apply-payment-env.sh
set -euo pipefail

ENV_FILE="${1:-/var/www/tgbot-mls9990777/.env}"

set_var() {
  local key="$1"
  local value="$2"
  if grep -q "^${key}=" "${ENV_FILE}"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "${ENV_FILE}"
  else
    echo "${key}=${value}" >> "${ENV_FILE}"
  fi
}

set_var PAYMENT_PROVIDER_TOKEN "390540012:LIVE:100585"
set_var COURSE_URL "https://t.me/+0tTS-z-oXqo3NWIy"
set_var COURSE_PRICE_KOPECKS "199000"

echo "OK:"
grep -E '^(PAYMENT_PROVIDER_TOKEN|COURSE_URL|COURSE_PRICE_KOPECKS)=' "${ENV_FILE}"

systemctl restart tgbot-mls9990777
systemctl is-active tgbot-mls9990777
