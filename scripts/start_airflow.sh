#!/bin/bash

scripts/set_environment.sh
scripts/create_airflow_variables.sh

airflow scheduler
