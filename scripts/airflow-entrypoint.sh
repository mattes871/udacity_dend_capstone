#!/usr/bin/env bash

TRY_LOOP="20"

wait_for_port() {
  local name="$1" host="$2" port="$3"
  echo "---------- Parameters: $1 $2 $3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

echo "---------- Waiting for port -----------------------------------"

# This does not work
# wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
#
# Lets wait hard coded for 10s
sleep 10

echo "---------- Init Airflow DB ------------------------------------"
airflow db init

echo "---------- Upgrading Airflow DB -------------------------------"
airflow db upgrade

# sleep 20
# airflow flower

echo "Creating dummy UI user (admin/admin)"
airflow users create --username admin --firstname Air --lastname Flow --role Admin --email airflow@airflow.com -p admin

echo "Checking for variables to add to Airflow"
if [[ -e /usr/local/airflow/variables/all.json ]]; then
         airflow variables -i /usr/local/airflow/variables/all.json
fi
echo "Checking for connections to add to Airflow"

/usr/local/bin/python -m pip install --upgrade pip
## Install dataclass backport for Python 3.6
pip install dataclasses

#pip install opentelemetry-api \
#            opentelemetry-instrumentation
#
#opentelemetry-bootstrap --action=install

airflow webserver
