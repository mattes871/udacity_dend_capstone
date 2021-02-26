# Capstone Project

## Project idea
If it wasn't for COVID, we would probably discuss much more about the effects of Climate Change and how to build Climate Models and on which basis. A tremendous part of this basis is data.  Thousands of stations around the globe are collecting a host of measures: Temperature, precipitation, humidity, cloud status, wind, air quality, etc.

This project is about bringing together these data sources in a common and simplified database with a common format and comparable definition of their measurements, so that interested amateur researchers can make use of the data, e.g. providing own dashboards displaying data from their local surroundings or more complex tasks like trying to forecast air pollution given climate KPIs.

As a first step towards such a database, I investigated Amazon's [Open Data Registry](https://registry.opendata.aws/) and found two interesting data sources, bringing together the measures from classical weather parameters with air pollution status. Many more can be found in the list, even though for now, they remain in the Outlook section.

The goal of this project is to provide a database containing a condensed set of most relevant KPIs from the two data sources for a basic time grid (i.e. daily data). So that amateur researchers can start working with the data - without being overwhelmed by sheer detail and complexity some of the data sources might offer.

The two data sources I selected cover basic weather KPIs and the measurement of air pollution. The data is compiled and provided by
* the [National Oceanic and Atmospheric Administration (NOAA)](https://www.noaa.gov) of the U.S.
  Department of Commerce
* and [OpenAQ](https://openaq.org/)

### NOAA dataset
**Scope**: 160,000 stations worldwide, partially dating back until 1763
**Measures**: 5 core KPIs (Min/Max temperature, precipitation, snowfall, snow depth), 50+ further KPIs
**Source platform**: AWS S3 bucket
**File structure**: One file per year, all in the same directory; current year's
file gets daily updates appended
**Format**: csv, csv.gz
**Downloads**: https://docs.opendata.aws/noaa-ghcn-pds/readme.html

See https://docs.opendata.aws/noaa-ghcn-pds/readme.html for more details.

### OpenAQ dataset
**Scope**: 160,000 stations worldwide, partially dating back until 1763
**Measures**: 5 core KPIs (Min/Max temperature, precipitation, snowfall, snow depth), 50+ further KPIs
**Source platform**: AWS S3 bucket
**File structure**: One file per station per day. Separate directory for every day since Nov 2013.
**Format**: ndjson, ndjson.gz
**Downloads**: https://openaq-fetches.s3.amazonaws.com/index.html

See https://openaq-fetches.s3.amazonaws.com/index.html for more details.

Access to both datasets is also possible via AWS CLI:
> aws s3 ls <bucket-name> --no-sign-request


The definition for the staging tables as well as the production tables is found
in [create_noaa_tables.sql](./dags/sql/create_noaa_tables.sql) and [create_openaq_tables.sql](./dags/sql/create_openaq_tables.sql)



## Scope of the Project

Setup a workflow that:
- downloads data from NOAA and OpenAQ into a Staging area
- inserts the data from staging area into a Postgresql database ensuring
  quality control and duplicate handling
- transforms and combines data from the two sources into meaningful entities
- stores the entities in a production database
- generates an exemplary table for reporting or analysis from the production data

Further requirements:
- the workflow can backfill the database with available historic data
- the workflow can catchup on data that was not (yet) available during past dag_runs
- the software components allow easy addition of new data sources to
  the workflow

Not in scope:
- Securing the workflow against disaster, i.e. changes in source data structure
- Hardening software and infrastructure against unwanted access
- Performance tuning for components and underlying container infrastructure


## Design Considerations [also a note to the reviewer]

After a first look at the data sources, it is clear that the amount of data already qualifies as Big Data. At least when taking into account all the available history.
At the same time, my current time budget is very tight and my employer - who is paying for this course - expects me to finish the degree as soon as possible. Hence, I am trying to balance my project aspirations with the choice of an infrastructure that allows fast development and provides an almost instantaneous response when triggering experiments. Another consideration are costs. Having exceeded my Udacity-provided AWS allowance already in November, a nice Redshift server and ECM cluster are not in the budget any longer.  

Taking these non-technical constraints into account, a local solution based on Docker Containers, Apache Airflow and a Postgresql server seems to offer both: Rapid development and enough headroom to deal with the amount of data and computation at hand. Once the project prototype is running, the platform-agnostic containerization also promises an easy migration path to more powerful machines in the clouds to cope with additional data sources and an increased number of users.

### Airflow 2.0
Airflow is one of the standard frameworks for orchestrating workflows in data
engineering. As Airflow is now also supporting Kubernetes, it seems to be a safe choice in terms of computational scalability. In my local implementation, however, Airflow has to make do with a LinearExecutor and maximum 10 Cores. I chose Airflow version 2.0 because of the much more reliable grouping of sub tasks in contrast to the older SubDAG feature.   

### Postgresql 12
Postgresql is my standard choice whenever there is no apparent reason to go for a more specialized non-SQL database. In this use-case, the goal is to provide a multi-purpose database, a flexible basis for reporting, analysis and extraction of data. Thus, a relational database seems a good choice. With further improved performance and scalability in versions 12 and above, Postgresql works well even on very large datasets - given that indexes and partitions are designed well.

### Docker & Docker Compose
Using Docker and 'docker-compose' is a very elegant and convenient way to set up
development environments with minimum effort but maximum reusability of existing
components and maximum portability to other hosts in mind.
While the main goal of containers in this project was to quickly set up an isolated,
well-defined and portable development environment. Such an environment also lends itself easier to a migration onto more powerful platforms.

It needs to be said, however, that the current configuration was set up with
ease of installation and debugging in mind. It is not optimized for performance
or intended for production use.


## The ETL Pipeline
The pipeline uses four stages:

### The data source on Amazon S3
In our example, these are the NOAA and OpenAQ buckets on Amazon S3.

### A local staging area for downloading the data from S3
I chose an ordinary Unix filesystem for the sake of costs and simplicity. This is only used for temporarily storing the data before ingesting it into the Postgres staging schema. No special performance requirements are needed here. For future scale ups, the location of the filesystem and the staging area of the database should be on the same machine or even same storage device.

Alternatively, the staging could be an HDFS storage with further processing done in Apache Spark.  The stage could even be skipped
completely and data be imported directly into a database like AWS Redshift, assuming that also other data sources reside on AWS S3 and can be fed directly into Redshift.

### A staging schema in the Postgresql Database
A staging schema in which to ingest the raw data from the filesystem. The
rationale is to do quality checks, cleansing and transformations on the raw data
while already having the "support" of the database functionality. All data read from the filesystem is ingested in its raw format, i.e. as raw text (binary fields need extra handling - but there is no binary content in the two selected data sources).

### A production schema in Postgresql
The final stage is the production schema that contains the transformed, quality-checked and properly typed data that is ready for use in reporting, analytics or aggregation for end-user applications.


# Installing and Running the Project

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
The project downloads data from public Amazon S3 buckets. Those buckets can be accessed without AWS credentials. However, this information came in last minute and all testing has been done using AWS credentials with an AWS_KEY and an AWS_SECRET.

## Installing the Repository
Once Docker + Docker Compose are installed and running on your system, you can
download the repository from GitHub: [Udacity Capstone Project / Climate Data](https://github.com/mattes871/udacity_dend_capstone)

Before starting docker-compose, you should define the following environment
variables:

> export AWS_KEY='<your-key-here>'
> export AWS_SECRET='<your-secret-here>'
> export AWS_SECRET_URI=<url-encoded version of the AWS_SECRET>

Once the AWS credentials are provided, go to the main project directory and execute

> docker-compose up

This will spawn a postgres database, the airflow webserver and the scheduler.

The Airflow UI will be available at

https://localhost:8080

Username is 'admin', same is the password.

Activate the *noaa_dag* and sit back while the data is downloaded and
processed.

## Repository Workflow

### Passing Secrets & Credentials
To keep things simple and secure, credentials (e.g. AWS key and secret) need to be set as environment variables in your local environment, i.e. in the environment from
where the `docker-compose up` is executed.  In my project folder, I created a
*set_environment.sh* script that does

> export AWS_KEY='<your-key-here>'
> export AWS_SECRET='<your-secret-here>'
> export AWS_SECRET_URI=<url-encoded version of the AWS_SECRET>

If you store this as a '.sh'-file, do not forget to exclude this file from git
(using .gitignore).

The 'docker-compose.yaml' picks these variables up and provides them for use
inside the docker containers.

No further credentials for external platforms are needed to run the project demo. Even the AWS credentials might not be necessary in case of publicly accessible S3 buckets. Nevertheless, it shows how to pass credentials into the container without writing them down in code.

### NOAA and OpenAQ DAGs
For each data source, a separate DAG is defined orchestrating the ETL process end-2-end.  The various configuration parameters for each data source are specified in a json file each. These configurations are loaded into Airflow variables when the Airflow webserver starts. Changes to the configuration require a restart of the webserver.

The DAGs are sharing the Unix Filesystem and the Postgresql database. However, the data is kept separate, using folders and database schemata, respectively.

#### Preparing the Staging Areas
The first step in the workflow for NOAA and OpenAQ is to create the Postgres schemata for staging and production as well as creating the corresponding tables - unless they already existed.  This is done by passing the data-source specific SQL files to the *CreateTablesOperator*.
Furthermore, the lookup table for the KPIs is created and filled with the relevant KPI information, if not yet in the table.

#### Downloading data from S3: NOAA
Downloading dimensional data and fact data is done separately. The dimensional data resides in three fixed-width text files. After download using the  *DownloadS3FileToStagingOperator*, the files need to be reformatted into a '|'-delimited *.csv* format, so that Postgresql's efficient *COPY_FROM* function can be employed.  Some of the text fields contain special characters, so that the reformatting needs to enclose strings in quotations marks and escape potentially hazardous characters. The reformatting happens with the *ReformatFixedWidthFileOperator*. The reformatted data is stored in: ./staging_files/noaa/dimensions_csv/

The NOAA fact data is the largest chunk of data among the two data sources. Past years are stored in a single file per year in *.csv.gz* format (approx. 100-200MB) containing about 30 - 40 Million records each.  The current year is in the same format and contains all records from Jan 1st until the current date.
Due to the amount of data in the file and the (by-default) daily schedule, it is desirable to download only the not-yet-downloaded records from S3 when running a daily update of our database. Hence, a new operator, *SelectFromNOAAS3ToStagingOperator*, is taking care of an incremental download as well as bulk-downloading past years in the first run of the pipeline. All fact files can be found in: ./staging_files/noaa/facts/

All *.csv* files should now be in the same format and ready for direct import into Postgresql.

#### Downloading data from S3: OpenAQ


#### Ingesting data from local Filesystem into Postgres
The *.csv* files from the local filesystem are loaded into Postgres using the *COPY_FROM* import functionality (*LocalStageToPostgresOperator*).

#### Quality Checks on Staging Data
Data quality checks should be performed on the data in the Postgres staging area. While there are many tools for data manipulation also on the filesystem level, it seems much more appropriate for large data volumes to do this step based on Postgres or other database platforms.
The *DataQualityOperator* implemented here, offers some basic test capabilities via a list of SQL statements and expected - or unexpected - values. Currently, four checks are defined in *helpers.DataQualityChecks*.
For a proper production system, more sophisticated tests would need to be implemented (e.g. checking the number of lines in the files downloaded and comparing that to the number of records ingested).

#### Transferring Data from Staging to Production
Most of the heavy lifting in terms of transformation, duplicate handling and adding information is happening here.  Utilizing the *PostgresOperator*, all of this is done on the database server. No extra passing of data into python is necessary. In case of bottlenecks, optimizing the Postgresql server definitely pays off.
For NOAA facts, the transformation code resides in *helpers.SqlQueries.transform_noaa_facts*.

Duplicate handling is done via Postgresql's "ON CONFLICT" clause. In case of a later migration to Amazon Redshift, this might cause some headache as Redshift does not support this.

#### Finally: Building an example datamart
With the data available now in the production schema, one can easily run aggregations over time, location and type of measurement. The repository contains a separate dag ('process_dims_and_facts_dag') to show how to use the production data
to deliver aggregates for reporting or analysis purposes. In the example, a
monthly aggregation for weather stations in Germany is computed that contains
the average min & max temperatures as well as the monthly precipitation. The
output is stored in the table 'production.ol_mthly_analytic_ger'.
Again, the main work happens on the Postgresql server only. The corresponding SQL is defined in *helpers.SqlQueries.aggregate_ger_monthly_data* is executed via the *PostgresOperator*.

### The Schedule
The pipeline is built to run daily batches. The scheduler is set to run daily around midnight by default but the DAG can handle other intervals, too. Of course, intra-daily would not make sense as the NOAA server provides data only on a daily schedule.
OpenAQ offers a "realtime" interface with much more frequent updates (at least hourly). But in the current implementation, I decided to first work with the daily aggregates only.

### Catchup and Backfills
The operators for NOAA fact and dimension data handle catchup and backfill
by themselves. Hence airflow's catchup parameter is set to *False*. The operators
also manage if the NOAA data did not receive an update before the scheduled run.  The next run will always try to load all not yet downloaded data from
NOAA.

Every year of NOAA data holds approximately 100-200MB in gzipped format,
depending on the number of KPIs measured and provided by the weather stations.
Hence, the current setup of the project defines the earliest NOAA data to be
downloaded as 2020-01-01. This can be changed in the 'data_available_from'
variable of the ./variables/noaa.json file.

Please note that when catching up on several decades of history, the data
processing time can be massive (hours to days depending on the platform Postgres
is running on).

## Outlook & Future Scenarios
Moving ahead from this prototype, the project could get more interesting if further data sources could be added. Be it additional weather stations, e.g. from ECA&D, measurements from new areas (there is an S3 data set with Ocean surface temperatures measured from Satellites) or even image data for combination with the weather station coordinates.
Further data sources would first of all require additional storage space and more compute power for the database server. Ideally, the whole setup could move closer to the data, e.g. onto AWS servers in region US-EAST-1 (assuming that all data is part of the AWS Open Data Programme), thus reducing download times significantly.
Planning for a 100x increase of the data volume - in terms of records - would also require to go for a distributed database setup, like the one Amazon is offering with Redshift. Similarly, when increasing the number of users significantly, replicating the data onto several database servers would help.


# References

For the data sources: [Open Data Registry](https://registry.opendata.aws/),
https://docs.opendata.aws/noaa-ghcn-pds/readme.html, https://openaq-fetches.s3.amazonaws.com/index.html.


Besides the usual documentation on [Python.org](http://python.org), [AWS](https://docs.aws.amazon.com/), [Docker](https://www.docker.com) and [Postgresql](https://www.postgresql.org/), I intensively utilised the following sources for problem solving and inspiration:


[Medium.com](https://medium.com)
Lots of inspiration and help from [Medium.com](https://medium.com).  A great
source to get code snippets and to gain understanding how things work. However,
experience also shows that these snippets need heavy customization to fit into
ones own project context.

Some favorite articles related to Docker, Docker Compose and Airflow:

* [Apache/Airflow and PostgreSQL with Docker and Docker Compose](https://towardsdatascience.com/apache-airflow-and-postgresql-with-docker-and-docker-compose-5651766dfa96)
* [6 Things To Know When Dockerizing Python Apps in Production](https://betterprogramming.pub/6-things-to-know-when-dockerizing-python-apps-in-production-f4701b50ca46)
* [Airflow Sensor to check status of DAG Execution](https://medium.com/@sunilkhaire17/airflow-sensor-to-check-status-of-dag-execution-225342ec2897)
* [Airflow Schedule Interval 101](https://towardsdatascience.com/airflow-schedule-interval-101-bbdda31cc463)

[Stackoverflow](https://stackoverflow.com)
One of the best sources for finding solutions to problems that other people already had. Be it Python, Postgresql, AWS, you name it. Makes you also feel good that others apparently run into the same problems ;-)

* [GitHub.com](https://github.com)
GitHub is full of interesting repositories. If searching for 'Udacity Data
Engineering Nanodegree Capstone', already many inspirational links will pop up.
However, there is no quality check and most of the repos out there do not work
out-of-the-box (especially not the ones for Udacity degrees).


# Sources

## docker-compose.yaml
Based on docker-compose.yaml proposed in [Apache/Airflow and PostgreSQL with Docker and Docker Compose](https://towardsdatascience.com/apache-airflow-and-postgresql-with-docker-and-docker-compose-5651766dfa96)

## Using Task Sensors
Code snippet applied to wait for external tasks.
[Dependencies between DAGs: How to wait until another DAG finishes in Airflow?](https://www.mikulskibartosz.name/using-sensors-in-airflow/)
