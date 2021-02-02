#!/bin/bash

## Using ENV variables

source set_environment.sh

## Define the necessary connections for airflow (delete previous definitions)
airflow connections -d --conn_id 'aws_credentials'
airflow connections -a --conn_id 'aws_credentials' --conn_uri
'aws://${AWS_KEY}:${AWS_SECRET_URI}@' --conn_extra '{"region_name": "${AWS_REGION}"}'

