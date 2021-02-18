# Capstone Project

## Installing the Repository

Download the repository from Github

>

For this project I decided to use docker to provide a platform agnostic result.
However, I am aware that the docker implementation for MacOs has some "features"
in regards to mapping users and access rights that might work differently on
other Unix systems.

After downloading, you need to provide your credentials in for accessing AWS S3:
Rename the file ./scripts/set_environment-template.sh to set_environment.sh and
fill in your account information. Be sure to provide the AWS_SECRET_URI as a
web encoded string!

Once the AWS credentials are provided, go to the main project directory and execute

> docker-compose up


This will spawn a postgres database, the airflow webserver and the scheduler.

The Airflow UI will be available at

https://localhost:8080

Username is 'admin', same is the password.

Activate the *climate_datamart_dag* and sit back while the data is downloaded and
processed.


## Defining the Project

Climate and Climate Change have been on the Agenda for more than two decades. A
lot of discussion has been going on, a lot of research has been conducted. At
the bottom of it all, however, are data measurements. Basic measurements of
climate conditions all over the planet: temperature, humidity, precipitation,
etc.

In this project, I show how to tap into two of these data sources and how to use
Airflow to setup a process that collects data from those data sources
and updates a Postgresql database on a daily basis.

The data sources are collected and provided by
* The European Climate Assessment & Dataset project (ECA&D)
* and the National Oceanic and Atmospheric Administration (NOAA) of the U.S.
  Department of Commerce

### ECA&D dataset
**Scope**: 20,000 stations, 65 countries, European + Mediterranean countries
**Source platform**: AWS S3 http site
**File structre**: One file per KPI
**Format**: .zip text-file with explanatory header
**Downloads**: https://www.ecad.eu/dailydata/predefinedseries.ph

### NOAA dataset
**Scope**: 160,000 stations worldwide, partially dating back until 1763
**Source platform**: AWS S3 bucket
**File structre**: One file per year, all in the same directory; current year's
file gets daily updates appended
**Format**: csv, csv.gz
**Downloads**: https://docs.opendata.aws/noaa-ghcn-pds/readme.html

Access to the NOAA dataset is also possible via AWS CLI:
> aws s3 ls noaa-ghcn-pds

See https://docs.opendata.aws/noaa-ghcn-pds/readme.html for more details.


All data sources used in this project are publicly available and
free of charge for educational purposes.

## Scope the Project and Gather Data

Setup a workflow that
- downloads the most recent data from NOAA onto a Staging area 
- transforms the data into meaningful entities
- stores the data in a database
- performas quality checks on the data
Further requirements:
- the workflow should be able to backfill past data and data that was missed due to unavailability of the source data.
- the solution architecture should allow to easily add new data sources into the
  workflow


Workflow should be:
- Download from S3 into a Staging Area (e.g. local)
- Conduct the ETL and store the data in a Postgresql Database
- Run essential data qualit checks
- Run a sample analytics use case to showcase the usefulness of the data
- Use Airflow for orchestrating the workflow

## Setup the infrastructure

### Secrets & Credentials
In this project, all access credentials are passed via environment variables.  We use a shell script to set these variables. The .git repository contains a template file of the script - the original script is not part of the repository (.gitignore).


### Access to S3 buckets on AWS
see former projects

### Setup workflow management with Airflow

I chose Airflow for the management of the workflow. In order to make the code as
easy to transfer as possible, Airflow and its components (database, scheduler,
webserver) are containerized.


## Explore and Assess the Data

## Define the Data Model

## Run ETL to Model the Data

## Complete Project Write Up


