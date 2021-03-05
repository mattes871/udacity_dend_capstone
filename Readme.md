# Capstone Project

## Project idea

If it wasn't for COVID, we would probably discuss much more about the effects of
Climate Change and how to build Climate Models and on which basis. A tremendous
part of this basis is data.  Thousands of stations around the globe are
collecting a host of measures: Temperature, precipitation, humidity, cloud
status, wind, air quality, etc.

This project is about bringing together these data sources in a common and
simplified database with a common format and comparable definition of their
measurements, so that interested amateur researchers can make use of the data,
e.g. providing own dashboards displaying data from their local surroundings or
more complex tasks like trying to forecast air pollution given climate KPIs.

As a first step towards such a database, I investigated Amazon's
[Open Data Registry](https://registry.opendata.aws/) and found two interesting data
sources, bringing together the measures from classical weather parameters with
air pollution status. Many more can be found in the list, even though for now,
they remain in the Outlook section.

### Project Goal & Purpose of the Data Model

The goal of this project is to provide a database containing a condensed set of
most relevant KPIs from the two data sources for a basic time grid (i.e. daily
data). So that amateur researchers can start working with the data - without
being overwhelmed by sheer detail and complexity some of the data sources might
offer.

The two data sources I selected cover basic weather KPIs and the measurement of
air pollution. The data is compiled and provided by

* the [National Oceanic and Atmospheric Administration (NOAA)](https://www.noaa.gov) of the U.S.
  Department of Commerce
* and [OpenAQ](https://openaq.org/)

#### NOAA dataset

* **Scope**: 160,000 stations worldwide, partially dating back until 1763
* **Measures**: 5 core KPIs (Min/Max temperature, precipitation, snowfall, snow
depth), 50+ further KPIs **Source platform**: AWS S3 bucket **File
structure**: One file per year, all in the same directory; current year's file
gets daily updates appended
* **Format**: csv, csv.gz
* **Downloads**: https://docs.opendata.aws/noaa-ghcn-pds/readme.html

See https://docs.opendata.aws/noaa-ghcn-pds/readme.html for more details.

#### OpenAQ dataset

* **Scope**: 8,000 stations in 90 countries with hourly to daily data back to 2013; > 1.5 Mio measurements per day
* **Measures**: 12 different KPIs and measurement units
Amount of Ozone, Particle matter (10, 2.5), Carbon Monoxide, Sulphur Dioxide, Nitrogen Dioxide
* **Source platform**: AWS S3 bucket
* **File structure**: Separate daily directories back until Nov 2013, each populated with sets of .ndjson-Files .
* **Format**: ndjson, ndjson.gz
* **Downloads**: https://openaq-fetches.s3.amazonaws.com/index.html

See https://openaq-fetches.s3.amazonaws.com/index.html and https://github.com/openaq/openaq-data-format for more details.

Access to both datasets is also possible via AWS CLI:
> aws s3 ls <bucket-name> --no-sign-request

### The Data Model

As this project aims at providing a harmonised and condensed set of most
relevant climate measurements to the amateur researcher, the main use case for
the actual data lies in aggregating and extracting portions of the data relevant
to the researchers' use cases.  This will mainly be filtering, aggregation and
regular (incremental) fetching of the data over various dimensions, like
geo-location, date and type of measurement.  As the required operations greatly
depend on the researchers' use cases, the data model should allow a good balance
between flexibility and efficient processing of data at large scale.  

I chose a Star-Schema model on a relational database as it provides the required
flexibility for selections and aggregations while it still manages large data
volumes if indexes and partitions of the model are well designed and maintained.
Assuming that in the future different users with different use cases will be
selecting, aggregating and fetching the data they need from this platform, this
flexibility is required. At least until becomes clear from actual usage, which
operations on the database are most needed. If it turns out that data amount of
data increased by a factor of 100 and 99% of usage is slicing the data by
date, kpi and country, a key-value redesign would make sense.

For the sake of simplicity (and thus in line with the project goal), the Data
Model in the project consists of only three different entities:

* Physical locations: the point of measurement incl. geo-coordinates
* Measurements:  the actual matter that is measured, its unit and its value
* Data Sources: So far only OpenAQ or NOAA, no metadata attached

These entities are distributed over one fact and four dimension tables tailored
for convenient access - especially filtering of the measurements. One of the
assumptions in the table design is that filtering will happen a lot on physical
locations, especially countries. That is why for example the *country_id* field
was added to the fact table even though it could be joined from the *d_stations*
table. Due to the small size of the *country_id* field (varchar(2)), the extra
storage space seemed justified in light of the more efficient filtering.

Regarding storage space: the current version of the database is not optimised
for most efficient storage. In a future optimisation, all identifier-fields, for
example the combination of *common_kpi_name* and *common_unit*, could be mapped
onto simple numbers (bigint or uuid), which would reduce the required space in
the facts table significantly. At the same time, it would also reduce
readability of the data when working with it.

Another design decision for the data model is the introduction of a lookup table
for the different measurement types and units. Apparently, KPI names and
measurement units can vary between data sources and often even within a source.
Hence, *d_kpi_names* provides a complete mapping from the source measures to the
harmonized names and units used in the production table. It also contains a
description for each entry that can include transformation rules, if such a step
is necessary. One example of this is the temperature data from NOAA. As NOAA
delivers only integer values for its measurements, temperatures are provided in
1/10 degrees Celsius, i.e. 203 meaning 20.3°C. As the production table features
a decimal format, temperatures are transformed to the more intuitive
decimal format.


The complete Data Dictionary including table structure, fields, data types,
links between fields, constraints and a description can be found in this
[Excel table](./data-dictionary.xlsx) or in this  
[semi-colon-delimited version](./data-dictionary.csv).


The actual SQL-definition for the production tables can be found in
[create_common_tables.sql](./dags/sql/create_common_tables.sql)

The same for the staging tables tables can be found in
[create_noaa_tables.sql](./dags/sql/create_noaa_tables.sql),  
[create_openaq_tables.sql](./dags/sql/create_openaq_tables.sql).


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


## Design Considerations

The project prototype is based on:
- Docker Containers for a reproducible environment
- Airflow 2.0 for the workflow management
- Postgresql 12 as a relational data storage and processing engine

Taking these non-technical constraints into account, a local solution based on
Docker Containers, Apache Airflow and a Postgresql server seems to offer both:
Rapid development and enough headroom to deal with the amount of data and
computation at hand. Once the project prototype is running, the
platform-agnostic containerisation also promises an easy migration path to more
powerful machines in one of the clouds to cope with additional data sources and
an increased number of users.

### Airflow 2.0

Airflow is one of the standard frameworks for orchestrating workflows in data
engineering. Its focus on Python and the availability of pre-defined Hooks and
Operators to connect to basically all of the popular data platforms and their
APIs, makes it an easy-to-start-with framework. In addition, the fact that the
actual processing is not done by Airflow but in the orchestrated components,
makes it lightweight and efficient enough to handle large production workflows.
If the workload increases, a scaling of the component can solve the problem and
might not require a redesign of the workflow as such.  As Airflow is now also
supporting Kubernetes, it seems to be a safe choice in terms of computational
scalability.  

For the project at hand, Airflow needs to manage the daily update of the
production database by orchestrating the download and processing of data from
different sources. Airflow already comes with most of the required Connectivity
and thus allows a very quick integration of new data sources. Dealing with
different sources also means dealing with different data formats. With its
natural focus on Python, handling various data formats (.csv, .json, gzipped
files, ...) can be done easily by using the whole universe of Python packages
and expertise (e.g. using boto3 for some special access to S3 that was not
available via the AWS Hooks). Talking about expertise, most Data Scientists and
Engineers are proficient in Python.

Reasons to choose Airflow 2.0:
* Workflow framework, actual work happens in components
* Lots of Operators and Hooks to connect to all popular Data Sources & APIs
* Focus on Python = quick learning curve + lots of existing code & expertise
* Platform agnostic

### Postgresql 12

Postgresql is my standard choice whenever there is no apparent reason to go for
a more specialized non-SQL database. In this use-case, the goal is to provide a
multi-purpose database: a flexible basis for reporting, analysis and extraction
of data. The data is coming in daily batches and no real-time requirements need
to be considered. Thus, a relational database is a good choice. With further
improved performance and scalability in versions 12 and above, Postgresql works
well even on very large datasets - given that indexes and partitions are
designed well.

In this project, Postgresql is the actual work horse as the aggregation and
transformation of data from the different sources is done using SQL. While this
is a matter of taste, SQL is an easy-to-understand and very well suited language
for the kinds of aggregations and transformations required in this project. Once
the data is loaded into the staging schema of Postgresql, the heavy lifting for
import process is defined in a few lines of easy-to-understand and thus
easy-to-maintain SQL statements.

Last but not least, another reason to go for Postgresql over other relational
databases, is its ability to automatically resolve conflicts in insertion mode
('ON CONFLICT' clause).


### Docker & Docker Compose

Using Docker and 'docker-compose' is an elegant and convenient way of setting up
development environments with minimum effort but maximum reusability of existing
components and maximum portability to other hosts in mind. While the main goal
of containers in this project was to quickly set up an isolated, well-defined
and portable development environment. Such an environment also lends itself
easier to a migration onto more powerful platforms.

It needs to be said, however, that the current configuration was set up with
ease of installation and debugging in mind. It is not optimised for performance
or intended for production use.


## The ETL Pipeline

The pipeline uses four stages:

### The data source on Amazon S3

In our example, these are the NOAA and OpenAQ buckets on Amazon S3.

### A local staging area for downloading the data from S3

I chose an ordinary Unix filesystem for the sake of costs and simplicity. This
is only used for temporarily storing the data before ingesting it into the
Postgres staging schema. No special performance requirements are needed here.
For future scale ups, the location of the filesystem and the staging area of the
database should be on the same machine or even same storage device.

Alternatively, the staging could be an HDFS storage with further processing done
in Apache Spark.  The stage could even be skipped completely and data be
imported directly into a database like AWS Redshift, assuming that also other
data sources reside on AWS S3 and can be fed directly into Redshift.

### A staging schema in the Postgresql Database

A staging schema in which to ingest the raw data from the filesystem. The
rationale is to do quality checks, cleansing and transformations on the raw data
while already having the "support" of the database functionality. All data read
from the filesystem is ingested in its raw format, i.e. as raw text (binary
fields need extra handling - however, this is out of scope in this project as
there is no binary content in the two selected data sources).

### A production schema in Postgresql

The final stage is the production schema that contains the transformed,
quality-checked and properly typed data that is ready for use in reporting,
analytics or aggregation for end-user applications.


# Installing and Executing the Project-demo

## Prerequisites

### Docker & Docker Compose
The project uses Docker containers to provide a platform agnostic setup.

To install and run the repository, you need Docker and Docker Compose installed
on your computer.  The project was implemented and tested with Docker Engine
v20.10.2 and Docker Compose 1.27.4 on MacOS 10.14.6. A separate deployment using
Docker v20.10.5 and Docker Compose 1.27.4 on Ubuntu 18.04.03 LTS worked
out-of-the-box.

For download and installation instructions of Docker and Docker Compose checkout
[Docker](https://docs.docker.com/engine/install/)
and [Docker Compose](https://docs.docker.com/compose/install/).

### AWS IAM role

The project downloads data from public Amazon S3 buckets. Those buckets can be
accessed without AWS credentials when using the AWS Command Line Interface.
However, this information came in last minute and all testing had been done
using AWS credentials with an AWS_KEY and an AWS_SECRET.  The current
implementation does not work without proper AWS credentials.

## Installing the Repository

Once Docker and Docker Compose are installed and running on your system, you can
download the repository from GitHub:
[Udacity Capstone Project / Climate Data](https://github.com/mattes871/udacity_dend_capstone)

Before starting docker-compose, you should define the following environment
variables:

> export AWS_KEY='<your-key-here>'
> export AWS_SECRET='<your-secret-here>'
> export AWS_SECRET_URI=<url-encoded version of the AWS_SECRET>

Alternatively, you can place these variables in the 'docker.env' file.

Once the AWS credentials are provided, go to the main project directory and execute

> docker-compose up

This will spawn a postgres database, the airflow webserver and the scheduler.
When launching for the first time, Airflow might raise warnings about missing
variables/keys. These can be safely ignored as the variables are loaded at a
later point.

The Airflow UI will be available at

https://localhost:8080

Username is 'admin', same is the password.

Activate the *noaa_dag* and the *openaq_dag* and sit back while the data is
downloaded and processed.

### Runtimes and Storage Requirements

With the exemplary settings of *data_available_from* in *./variables/noaa.json*
and *./variables/openaq.json*, the initial run of the DAGs took about 1-2 hours
on a current workstation with 10 cores, 16 GB, SSD mass storage and 100MBit/sec.
internet connection. In addition, one needs approx 150GB of free disk space
for storage of the downloaded files and the database tables.

For only a quick test with reduced amount of data, change the settings for
*data_available_from* in the files *./variables/noaa.json* and
*./variables/openaq.json* to '2021-02-01'. This is a decent value that brings
down the time for download and execution significantly while still fulfilling
the requirement to process more than 1 Million records.


## Repository Workflow

### Executing the DAGs
The project demo consists of three DAGs:
- noaa_dag
- openaq_dag
- process_example_dag

*noaa_dag* and *openaq_dag* are independent of each other and can run in
arbitrary order. A parallel run is possible but might not be recommendable in a
local setup. Once the Airflow UI is running, one can switch on both DAGs and
wait until the first run is done. Be aware that downloading and processing all
the historic data can take a very long time. Subsequent executions will take
much less time.

The *process_example_dag* should be executed on demand and only after the
*noaa_dag* ran successfully.


### Passing Secrets & Credentials

To keep things simple and secure, credentials (e.g. AWS key and secret) need to
be set as environment variables in your local environment, i.e. in the
environment from where the `docker-compose up` is executed.  In my project
folder, I created a *set_environment.sh* script that does

> export AWS_KEY='<your-key-here>'
> export AWS_SECRET='<your-secret-here>'
> export AWS_SECRET_URI=<url-encoded version of the AWS_SECRET>

If you store this as a '.sh'-file, do not forget to exclude the file from git
(using .gitignore).

If git is not a concern, you can also add those credentials directly to the
'docker.env' file

The 'docker-compose.yaml' picks up these variables and provides them for use
inside the docker containers.

No further credentials for external platforms are needed to run the project
demo. Even the AWS credentials might not be necessary in case of publicly
accessible S3 buckets. Nevertheless, it shows how to pass credentials into the
container without writing them down in code.

Please note, that the Postgresql database is provided in a rudimentary
installation with only the *airflow* user set up - for the purpose of this demo
project.  For a production deployment, some more work needed to be invested
here.

### NOAA and OpenAQ DAGs

For each data source, a separate DAG is defined orchestrating the ETL process
end-2-end.  The various configuration parameters for each data source are
specified in a json file each. These configurations are loaded into Airflow
variables when the Airflow webserver starts. Changes to the configuration
require a restart of the webserver.

The DAGs are sharing the Unix Filesystem and the Postgresql database. However,
the data is kept separate, using folders and database schemata, respectively.

#### Preparing the Staging Areas

The first step in the workflow for NOAA and OpenAQ is to create the Postgres
schemata for staging and production as well as creating the corresponding tables
- unless they already existed.  This is done by passing the data-source specific
SQL files to the *CreateTablesOperator*. Furthermore, the lookup table for the
KPIs is created and filled with the relevant KPI information, if not yet in the
table.

#### Downloading data from S3: NOAA

Downloading dimensional data and fact data is done separately. The dimensional
data resides in three fixed-width text files. The files are updated by NOAA on
an on-demand basis, i.e. only when the dimension data changed. Hence, the
*DownloadS3FileToStagingOperator* first checks if the file on S3 is more recent
than on local Staging and downloads the S3 file if that is the case. The files
then need to be reformatted into a '|'-delimited *.csv* format, so that
Postgresql's efficient *COPY_FROM* function can be employed.  Some of the text
fields contain special characters, so that the reformatting needs to enclose
strings in quotations marks and escape potentially hazardous characters. The
reformatting happens with the *ReformatFixedWidthFileOperator*. The reformatted
data is stored in: ./staging_files/noaa/dimensions_csv/

The NOAA fact data is the largest chunk of data among the two data sources. Past
years are stored in a single file per year in *.csv.gz* format (approx.
100-200MB) containing about 30 - 40 Million records each.  The current year is
in the same format and contains all records from Jan 1st until the current date.
Due to the amount of data in the file and the (by-default) daily schedule, it is
desirable to download only the not-yet-downloaded records from S3 when running a
daily update of our database. Hence, a new operator,
*SelectFromNOAAS3ToStagingOperator*, is taking care of an incremental download
as well as bulk-downloading past years in the first run of the pipeline. All
fact files can be found in: ./staging_files/noaa/facts/

All *.csv* files should now be in the same format and ready for direct import
into Postgresql.

#### Downloading data from S3: OpenAQ


#### Ingesting data from local Filesystem into Postgres

The *.csv* files from the local filesystem are loaded into Postgres using the
*COPY_FROM* import functionality (*LocalCSVToPostgresOperator*). Currently,
there is a separate staging schema for each data source (*openaq_schema* and
*noaa_schema* respectively) but the table names are static. Thus, all tasks are
loading data into the same tables. A future extension of this could be to add
the *execution_date* as a suffix to each table or schema name, thus eliminating
any chance of conflict between concurrent DAG runs or data that remained from an
unsuccessful run of an operator. In the current version, we do not permit
concurrent execution of tasks from different execution dates to avoid conflicts.

Due to the restriction in concurrent execution, it is possible to safely drop
all data that is already/still in the tables before the ingestion process
starts. Thus it is possible to keep the data in the staging tables even after
successful processing for the sake of debugging without penalty on actual
processing time (see also later paragraph on "(Not) Cleaning up Staging table").

#### Quality Checks on Staging Data

Data quality checks should be performed on the data in the Postgres staging
area. While there are many tools for (fast) data manipulation also on the
filesystem level, it seems much more appropriate for large data volumes to do
this step based on Postgres or other database platforms.  The two data quality
operators implemented so far, offer basic test capabilities for completeness of
loaded data and the validity of field contents.

The *CompletenessCheckFilesVsPostgres* compares the number of lines in a (set
of) text files against the number of records in a Postgresql table. If the
ingest process went well, both numbers should be the same (minus the number of
header lines). Otherwise, the loading process went wrong and the operator raises
an *AssertionError*.

The *DataQualityOperator* takes a list of SQL statements and their expected - or
unexpected - outcome values. Currently, five different checks on NOAA and OpenAQ
data are defined in *helpers.DataQualityChecks*. Unexpected outcomes raise *ValueErrors*.

For a proper production system, more sophisticated
tests would need to be implemented (e.g. checking the number of lines in the
files downloaded and comparing that to the number of records ingested).

#### Transferring Data from Staging to Production

Most of the heavy lifting in terms of transformation, duplicate handling and
adding information is happening here.  Utilising the *PostgresOperator*, all of
this is done on the database server. No extra passing of data into python is
necessary. In case of bottlenecks, optimising the Postgresql server definitely
pays off.

It needs to be mentioned that NOAA and OpenAQ data share the same fact and
dimension tables in production. Transferring the data from staging to the
production schema requires a harmonisation of both sources' data.

##### Dimensional data for NOAA

The content of the dimensional tables from staging is inserted into the
production tables. Duplicates are avoided by using Postgresql's 'ON CONFLICT'
mechanism. For weather stations and countries, duplicate keys trigger result in
skipping the insert action ('DO NOTHING'). In case of the  inventory data, the
existing record is updated with the most recent data from staging.

##### Dimensional data for OpenAQ

The information about the measurement points is extracted by aggregating this
data from the OpenAQ fact table in staging. In analogy to NOAA, these records
are then passed to production for insertion. Duplicates are skipped using
Postgresql's 'ON CONFLICT' clause.

##### Facts data from NOAA

NOAA provides daily measurements. The main transformation is to convert the
string values to their proper data types (according to the data model) The
transformation code for NOAA resides in
*helpers.SqlQueries.transform_noaa_facts*.

##### Facts data from OpenAQ

In contrast to NOAA, most of OpenAQ's measurements are on an hourly basis. For
the sake of simplicity, the production data should be of daily granularity. In
addition to the type conversions, the transformation for OpenAQ includes an
aggregation step from hourly to daily. The exemplary transformation code for an
arithmetic-mean aggregation is provided in
*helpers.SqlQueries.transform_openaq_avg_facts*. Several other meaningful ways
of aggregation can be thought of as future enhancements: Min and max values,
median, moving average, etc.

Remark on 'ON CONFLICT': Duplicate handling is done via Postgresql's "ON
CONFLICT" clause. In case of a later migration to Amazon Redshift, this might
cause some headache as Redshift does not support this.


#### (Not) Cleaning up Staging Tables after successful transfer

During the implementation of the workflow, it is reasonable to keep the data on
staging even after a (seemingly) successful transfer to production tables for
the sake of easier debugging. In a production setting, the staging tables should
be truncated after all transfers have been completed successfully to make space
for new data.

Thanks to the integrated duplicate-handling, the current version could also
manage if already imported data remains in the staging tables. Due to the amount
of data, however, not too many runs should be accumulated to keep processing
times bearable.


#### Finally: Building an example datamart

With the data available in the production schema after the NOAA- and OpenAQ-DAG
ran, one can easily conduct aggregations over time, location and type of
measurement. The repository contains a separate dag ('process_example_dag') to
show how to use the production data to deliver aggregates for reporting or
analysis purposes. In the example, a monthly aggregation for weather stations in
Germany is computed that contains the average min & max temperatures as well as
the monthly precipitation. The output is stored in the table
'production.ol_mthly_analytic_ger'. Again, the main work happens on the
Postgresql server. The corresponding SQL is defined in
*helpers.SqlQueries.aggregate_ger_monthly_data* and is executed via the
*PostgresOperator*.

### The Schedule

The pipeline is built to run daily batches. The scheduler is set to run daily
shortly before midnight by default but the DAG can handle other intervals, too.
Of course, intra-daily would not make sense as the NOAA server provides data
only on a daily schedule. OpenAQ offers a "realtime" interface with much more
frequent updates (at least hourly). But in the current implementation, I decided
to provide only daily data for the sake of simplicity.

A remark on *execution_date* and downloads for OpenAQ: To ensure that only
complete daily data is downloaded, the operators use the *{{ yesterday_ds }}*
macro. Using the actual execution date typically leads to incomplete downloads
as the data still keeps coming in.


### Catchup and Backfills

#### NOAA

The operators for NOAA fact and dimension data handle catchup and backfill by
themselves. Hence airflow's catchup parameter is set to *False*. The operators
also manage if the NOAA data did not receive an update before the scheduled run.
The next run will always try to load all not yet downloaded data from NOAA.

Every year of NOAA data holds approximately 100-200MB in gzipped format,
depending on the number of KPIs measured and provided by the weather stations.
Hence, the current setup of the project defines the earliest NOAA data to be
downloaded as 2020-01-01. This can be changed in the 'data_available_from'
variable of the ./variables/noaa.json file.

Please note that when catching up on several decades of history, the data
processing time can be massive (hours to days depending on the platform Postgres
is running on).

#### OpenAQ

The OpenAQ data lends itself easily to a daily download scheme as all input from
the measurement stations is stored in a separate folder per day on S3. Hence, I
am using Airflow's ability to backfill historic data by setting the *start_date*
to the first date, from which we want to ingest measurements into the database.


## Outlook & Future Scenarios

Moving ahead from this prototype, the project could get more interesting if
further data sources could be added. Be it additional weather stations, e.g.
from ECA&D, measurements from new areas (there is an S3 data set with Ocean
surface temperatures measured from Satellites) or even image data for
combination with the weather station coordinates.

### Shortcomings of the Prototype and required steps towards a Production System

The current Postgresql Container is multi-user capable but currently set up with
a single standard user (*airflow*) whose access credentials are accessible
within the project repository. If the container got deployed in a less secure
environment, attackers would have an easy game of getting admin rights to the
whole database (e.g. by guessing the password)

Measure: Harden Postgresql Database / Container, e.g. by restricting access to
Postgresql via the configuration pg_hba.cfg. A great list of measures is
provided in the EDB Blog on
[How to secure Postgresql: Security Hardening Best Practices & Tips](https://www.enterprisedb.com/blog/how-to-secure-postgresql-security-hardening-best-practices-checklist-tips-encryption-authentication-vulnerabilities)

A future scenario where dozens or hundreds of users have accounts on the
database machine and might use them concurrently, would require a massive scale
up of the number of database connections possible and thus an upgrade of the
underlying database infrastructure.

Measure: Moving to a distributed database setup and introduction of replication
to avoid deadlocks.

Although the containers can be easily used on any Virtual Machine, the current
setup is not optimised around scalability of the underlying infrastructure but
relies on a simply big-enough machine to execute the containers. In case of a
100x increase in data volume (in terms of records), the current single-container
setup would no longer work.

Measure: Move to a more dynamically scalable infrastructure (Cloud + Kubernetes)
in addition to changing the database setup to a distributed system with higher
replication.

The prototype tries to keep download volumes as low as possible. Still
downloading can take a significant amount of time especially when catching up on
missed time intervals, e.g. depending on the time of day, download times for one
month of daily OpenAQ files varied between 5 minutes to one hour (Downloading
from US-East-1 to Europe).

Measure: Move the infrastructure closer to the data, e.g. onto AWS servers in
region US-EAST-1 (assuming that all data is part of the AWS Open Data
Programme), thus reducing download times significantly.




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

## wait_for_postgres.sh

A script for docker's entrypoint applicable to the Airflow webserver and scheduler containers, to wait until the Postgresql database is running and ready to accept connections. Version with own adaptations taken from [wait_for_postgres.sh](https://gist.github.com/zhashkevych/2742682ab57b5670a15291864658625b)

## Solving permission issues with Staging folder in Docker Container

When using *Volumes* in a Docker Container, write permissions are a frequent issue.  Depending on how a Docker Image was built, mounting a volume will result in user and group to be 'root:root' inside the container. This can cause severe headaches when trying to access the volume - especially when following the security advice to run the container as a non-root user. I ran into this problem when using the official apache/airflow2.0 docker image. Inspiration for an easy and reliable workaround came from [Docker volume mount as non root](https://bhadrajatin.medium.com/docker-volume-mount-as-non-root-77ffae5a79d0).

## Using Task Sensors (but eventually removed from actual use in the project)

Code snippet applied to wait for external tasks.
[Dependencies between DAGs: How to wait until another DAG finishes in Airflow?](https://www.mikulskibartosz.name/using-sensors-in-airflow/)
