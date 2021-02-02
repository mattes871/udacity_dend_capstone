# Capstone Project

## Defining the Project

Ideation: 
Weather and Climate are interesting and highly relevant areas for which a lot
of data is available.  However, in many cases, this data cannot be mapped
directly onto other datasets that might be of interest.

E.g. ECA&D data from Europe (continent)
ECA&D is the European Climate Assessment & Dataset project. It presents information on changes in weather and climate extremes, as well as the daily dataset needed to monitor and analyse these extremes.
Bulk downloads are available at
https://www.ecad.eu/dailydata/predefinedseries.php and can also be accessed via
AWS S3 buckets

Idea update:
NOAA data is available via S3 buckets:
aws s3 ls noaa-ghcn-pds
See https://docs.opendata.aws/noaa-ghcn-pds/readme.html for more details.

Workflow should be:
- Download from S3 into a Staging Area (e.g. local)
- Conduct the ETL and store the data in a Postgresql Database
- Run essential data qualit checks
- Run a sample analytics use case to showcase the usefulness of the data
- Use Airflow for orchestrating the workflow

## Scope the Project and Gather Data

Connect to ECA&D S3-bucket and download (a sample) of the datConnect to ECA&D
S3-bucket and download (a sample) of the dataa

## Setup the infrastructure

### Secrets & Credentials
In this project, all access credentials are passed via environment variables.  We use a shell script to set these variables. The .git repository contains a template file of the script - the original script is not part of the repository (.gitignore).


### Access to S3 buckets on AWS
see former projects

### Setup workflow management with Airflow

I chose Airflow for the management of the workflow. In order to make the code as
easy to transfer as possible, Airflow and its components (database, scheduler,
webserver) should run in a docker container.


## Explore and Assess the Data

## Define the Data Model

## Run ETL to Model the Data

## Complete Project Write Up

