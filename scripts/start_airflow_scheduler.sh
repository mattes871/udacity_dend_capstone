#!/bin/bash


scripts/set_environment.sh

# Wait and make sure the Web Server is up and running
sleep 30

scripts/create_airflow_variables.sh

airflow scheduler #-D

