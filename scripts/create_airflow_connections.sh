#!/bin/bash

## Using ENV variables
## AWS environment variables should be defined in docker-compose.yaml
## or docker.env file

## Define the necessary connections for airflow (delete previous definitions)

# echo "airflow connections add \
#         --conn-uri 'aws://${AWS_KEY}:${AWS_SECRET_URI}@' \
#         --conn-extra '{region_name: ${AWS_REGION}}' \
#         'aws_credentials'"

airflow connections delete 'aws_credentials'
airflow connections add \
        --conn-uri "aws://${AWS_KEY}:${AWS_SECRET_URI}@" \
        --conn-extra '{"region_name": "eu-central-1"}' \
        'aws_credentials'

airflow connections delete 'postgres'
airflow connections add \
        --conn-uri \
        "postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:5432/${POSTGRES_DB}" \
        'postgres'

# airflow connections add \
#         --conn-uri 'postgres://airflow:airflow@postgres:5432/airflow' \
#         'postgres'
