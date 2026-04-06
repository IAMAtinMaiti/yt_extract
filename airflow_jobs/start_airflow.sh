#!/usr/bin/env zsh

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

export AIRFLOW_HOME="$PROJECT_ROOT/airflow_jobs/"
export AIRFLOW_CONFIG="$AIRFLOW_HOME/configs/airflow.cfg"

JWT_SECRET_FILE="$AIRFLOW_HOME/temp/config/jwt_secret"

mkdir -p \
  "$AIRFLOW_HOME/temp/logs" \
  "$AIRFLOW_HOME/temp/plugins" \
  "$AIRFLOW_HOME/temp/config" \
  "$PROJECT_ROOT/airflow_jobs/datalake"

cd "$PROJECT_ROOT/airflow_jobs"

if [[ -z "${AIRFLOW__API_AUTH__JWT_SECRET:-}" ]]; then
  if [[ ! -s "$JWT_SECRET_FILE" ]]; then
    python3 - <<'PY' > "$JWT_SECRET_FILE"
import secrets

print(secrets.token_urlsafe(64))
PY
    chmod 600 "$JWT_SECRET_FILE"
  fi

  jwt_secret="$(<"$JWT_SECRET_FILE")"
  export AIRFLOW__API_AUTH__JWT_SECRET="$jwt_secret"
fi

# Create or upgrade the Airflow metadata database on every start.
airflow db migrate

exec airflow standalone
