#!/usr/bin/env bash

#airflow tasks test noaa_dimension_dag Create_noaa_dim_tables 2020-01-31

airflow tasks test noaa_dimension_dag Copy_noaa_dim_file_to_staging 2020-01-31
