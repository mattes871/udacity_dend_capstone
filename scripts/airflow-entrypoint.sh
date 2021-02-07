#!/usr/bin/env bash


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

airflow webserver -D && airflow scheduler -D

