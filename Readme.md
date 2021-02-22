# Capstone Project

## Note to the Reviewer

Aspiration level of this project is a minimal prototype that demonstrates the
different skills we learned in this Nanodegree. Hence, I am demonstrating how to
setup a pipeline using the following tools:
- Airflow to define and orchestrate the pipeline
- Amazon S3 as the primary data source
- Postgresql for defining the data model and storing the intermediate data
- Postgresql for defining a data mart and storing dashboard/report data

Not in scope of this project is securing the prototype against changes in the
data and other mistakes that needed to be taken into account for a real
production system. Hence, I am deliberately skipping all kinds of extras that
should be included for a proper testing (assert, try/except).  Furthermore, the
docker-compose approach exhibits some performance issues when using a local
macOS host. For the sake of simplicity and easy testing, I chose a setup that is
not optimal in terms of performance but sufficient to demonstrate the project
(e.g. mounting multiple directories from the local machine into the containers).


## Installing the Repository

Download the repository from Github

> git clone 

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

### ECA&D dataset
**Scope**: 20,000 stations, 65 countries, European + Mediterranean countries
**Source platform**: AWS S3 http site
**File structre**: One file per KPI
**Format**: .zip text-file with explanatory header
**Downloads**: https://www.ecad.eu/dailydata/predefinedseries.ph

## Scope the Project and Gather Data

Setup a workflow that
- downloads data from NOAA into a Staging area
- Inserts the data from staging area into a Postgresql DB ensuring
  quality control and duplicate handling
- transforms the data into meaningful entities
- stores the entities in a database
- performas quality checks on the data
Further requirements:
- the workflow should be able to backfill past data and data that was missed due to unavailability of the source data.
- the solution architecture should allow to easily add new data sources into the
  workflow


Workflow should be:
- Download fact files from S3 into a Staging Area (e.g. local)
- Download documentation and dimension files into a Staging Area if
  the files are newer than current files on Staging
- Conduct the ETL and store the data in a Postgresql Database
- Run essential data qualit checks
- Run a sample analytics use case to showcase the usefulness of the data
- Use Airflow for orchestrating the workflow

## Setup the infrastructure

### Secrets & Credentials
All access credentials need to be set/exported in the shell that executes the
docker-compose up command. 

Unfortunately, this is also the reason why the airflow connections use a
different mechanism for their setup than the airflow variables, which are loaded
from a .json file.


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


