#!/bin/bash

# echo "Sourcing set_environment.sh"
# source ${AIRFLOW_HOME}/scripts/set_environment.sh

#${AIRFLOW_HOME}/scripts/create_airflow_variables.sh

airflow scheduler -D

