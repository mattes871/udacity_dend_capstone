#!/usr/bin/env bash


echo "---------- Init Airflow DB ------------------------------------"
airflow db init

echo "---------- Upgrading Airflow DB -------------------------------"
airflow db upgrade

# sleep 20
# airflow flower

echo "Creating UI user (admin/admin)"
airflow users create --username admin --firstname Air --lastname Flow --role Admin --email airflow@airflow.com -p admin

echo "Checking '${AIRFLOW_HOME}' for variables to add to Airflow"
for file in ` ls ${AIRFLOW_HOME}/variables/*.json `
do
        echo "Importing ${file}"
        airflow variables import ${file}
done
#if [[ -e ${AIRFLOW_HOME}/variables/noaa.json ]]; then
#        echo "Importing airflow variables from ${AIRFLOW_HOME}/variables/noaa.json"
#        airflow variables import ${AIRFLOW_HOME}/variables/noaa.json
#fi

# Use shell script to create connections, so that credentials
# can be passed via Environment variables instead of a json file
${AIRFLOW_HOME}/scripts/create_airflow_connections.sh

airflow webserver -D
