#!/usr/bin/env bash


echo "---------- Init Airflow DB ------------------------------------"
airflow db init

echo "---------- Upgrading Airflow DB -------------------------------"
airflow db upgrade

# sleep 20
# airflow flower

echo "Creating UI user (admin/admin)"
airflow users create --username admin --firstname Air --lastname Flow --role Admin --email airflow@airflow.com -p admin

echo "Checking for variables to add to Airflow"
if [[ -e ${AIRFLOW_HOME}/variables/noaa.json ]]; then
        echo "Importing airflow variables from ${AIRFLOW_HOME}/variables/noaa.json"
        airflow variables import ${AIRFLOW_HOME}/variables/noaa.json
fi
echo "Checking for connections to add to Airflow"
#if [[ -e /usr/local/airflow/variables/connections.json ]]; then
#         airflow connections import /usr/local/airflow/variables/connections.json
#fi
# Use shell script to create connections, so that credentials
# can be passed via Environment variables instead of a json file
${AIRFLOW_HOME}/scripts/create_airflow_connections.sh

#echo "Sourcing set_environment.sh"
#source ./scripts/set_environment.sh

airflow webserver -D


