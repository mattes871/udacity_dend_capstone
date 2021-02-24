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


## Prerequisites 

### Docker & Docker Compose
The project uses Docker containers to provide a platform agnostic setup.

To install and run the repository, you need Docker and Docker Compose installed
on your computer.  The project was implemented and tested with Docker Engine
v20.10.2 and Docker Compose 1.27.4 on MacOS 10.14.6.

For download and installation instructions checkout for
[Docker](https://docs.docker.com/engine/install/) 
and for [Docker Compose](https://docs.docker.com/compose/install/).

### AWS IAM role
The project downloads data from a public Amazon S3 bucket. In order to access
the data, an AWS IAM role with access to S3 is required. For the actual access,
you need the AWS_KEY and the AWS_SECRET.

## Installing the Repository
When Docker and Docker Compose are installed and running on your system, you can
download the repository from GitHub:

> git clone 

Before starting docker-compose, you need to define the following environment
variables: 

> export AWS_KEY='<your-key-here>'
> export AWS_SECRET='<your-secret-here>'

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

### Re-Scoping the Project after two weeks
While implementing the Pipeline for the NOAA data source it became apparent that
one data source alone is more than enough effort for the time available. Hence,
I decided to rescope the project and restrict it to deal with a single data
source only and at the same time the project is still complex enough to
demonstrate the different tools and concepts we learned in this Nanodegree.

## Setting up the infrastructure

### Docker & Docker Compose
Using Docker and 'docker-compose' is a very elegant and convenient way to set up
development environments with minimum effort and maximum reusability of existing
components and maximum portability to other hosts. In this project,
'docker-compose' allowed me to combine a Postgresql 12 database with the newest
Apache Airflow 2.0 for orchestration of the pipeline.  

It needs to be said, however, that the current configuration was set up with
ease of installation and debugging in minde. It is not optimized for performance
or intended for production use. 

### Secrets & Credentials
For this project, you will need an AWS IAM profile with a *Key* and a *Secret*
to access the NOAA data resource on Amazon S3.
To keep things simple and secure, the Key and the Secret need to be set as
environment variables in your local environment, i.e. in the environment from
where the `docker-compose up` is executed.  In my project folder, I created a
*set_environment.sh* script that does

> export AWS_KEY='<your-key-here>'
> export AWS_SECRET='<your-secret-here>'

If you store this as a '.sh'-file, do not forget to exclude this file from git
(using .gitignore).

The 'docker-compose.yaml' picks these variables up and provides them for use
inside the docker containers.

No further credentials for external platforms is needed to run the project demo.


### Access to S3 buckets on AWS
The NOAA data resides on Amazon S3 and requires an AWS IAM account for
retrieval. In order to limit the data transfer, the operators for downloading
dimension and fact data from NOAA use mechanisms to check whether more recent
data is available on NOAA - compared to the local downloads.

### Orchestrating workflow management with Airflow
Airflow is one of the standard frameworks for orchestrating workflows in data
engineering. I chose Airflow 2.0 for this project to take advantage of the newly
introduced TaskGroup concept, which offers significant upsides  over the usage
of SubDAGs. 

## Explore and Assess the Data

## Define the Data Model

## Run ETL to Model the Data

### Catchup and Backfill
The operators for NOAA fact and dimension data handle catchup and backfill
by themself. Hence catchup is set to *False*. The operators also take into
account if any of the runs does not yield new data. The next run will always try
to load all not yet downloaded data from NOAA.

Every year of NOAA data holds approximately 100MB in gzipped format. Hence, the
current setup of the project defines the earliest date of NOAA data to be
downloaded to be 2018-01-01. This can be changed in the 'data_available_from'
variable of the ./variables/noaa.json file.



## Complete Project Write Up


