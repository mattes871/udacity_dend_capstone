from datetime import date, datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from operators.create_tables import CreateTablesOperator
from operators.select_from_noaa_s3_to_staging import SelectFromNOAAS3ToStagingOperator
from operators.local_stage_to_postgres import LocalStageToPostgresOperator

from helpers.sql_queries import SqlQueries
# from helpers.source_data_class import SourceDataClass
AWS_KEY    = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
AWS_REGION = os.environ.get('AWS_REGION', default='eu-central-1')

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')

## noaa_config needs to be defined when starting Airflow
## Definition resides in ./variables/noaa.json
#
#  Define all required noaa config parameters here
#  because access to the airflow Variables via "Variables.get"
#  accesses the Airflow metadata database.
noaa_config: dict = Variable.get("noaa_config", deserialize_json=True)
NOAA_AWS_CREDS: str = noaa_config['source_params']['aws_credentials']
NOAA_S3_BUCKET: str = noaa_config['source_params']['s3_bucket']
NOAA_S3_KEYS: list = noaa_config['source_params']['s3_keys']
NOAA_S3_FACT_DELIM: str  = noaa_config['source_params']['fact_delimiter']
NOAA_S3_FACT_PREFIX: str = noaa_config['source_params']['s3_fact_prefix']
NOAA_FACT_COMPRESSION: str = noaa_config['source_params']['fact_compression']
NOAA_FACT_FORMAT: str = noaa_config['source_params']['fact_format']
NOAA_DATA_AVAILABLE_FROM: str = noaa_config['data_available_from']
NOAA_STAGING_LOCATION: str = os.path.join(AIRFLOW_HOME, noaa_config['staging_location'])

NOAA_S3_DIM_DELIM = '|'
NOAA_STAGING_FACTS = os.path.join(NOAA_STAGING_LOCATION,'facts')
NOAA_QUOTATION_CHAR = '"'

DEFAULT_START_DATE = datetime.now()

## 'postgres' is the name of the Airflow Connection to the Postgresql 
POSTGRES_STAGING_CONN_ID = os.environ.get('POSTGRES_HOST')
POSTGRES_CREATE_FACT_TABLES_FILE = 'dags/sql/create_facts_tables.sql'

print(f"""Environment for noaa_dimensions_dag:
NOAA_AWS_CREDS: {NOAA_AWS_CREDS}
NOAA_S3_BUCKET: {NOAA_S3_BUCKET}
NOAA_S3_KEYS: {NOAA_S3_KEYS}
NOAA_S3_FACT_DELIM: {NOAA_S3_FACT_DELIM}
NOAA_S3_FACT_PREFIX: {NOAA_S3_FACT_PREFIX}
NOAA_FACT_COMPRESSION: {NOAA_FACT_COMPRESSION}
NOAA_FACT_COMPRESSION: {NOAA_FACT_COMPRESSION}
NOAA_FACT_FORMAT: {NOAA_FACT_FORMAT}
NOAA_DATA_AVAILABLE_FROM: {NOAA_DATA_AVAILABLE_FROM} 
NOAA_STAGING_LOCATION: {NOAA_STAGING_LOCATION}
NOAA_S3_DIM_DELIM: {NOAA_S3_DIM_DELIM}
NOAA_STAGING_FACTS: {NOAA_STAGING_FACTS}
NOAA_QUOTATION_CHAR: {NOAA_QUOTATION_CHAR}
POSTGRES_STAGING_CONN_ID: {POSTGRES_STAGING_CONN_ID}
POSTGRES_CREATE_FACT_TABLES_FILE: {POSTGRES_CREATE_FACT_TABLES_FILE} 
""")


DEFAULT_ARGS = {
    'owner': 'matkir',
    'depends_on_past': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': DEFAULT_START_DATE,
    'region': AWS_REGION
}

def get_date_of_most_recent_noaa_facts() -> None:
    """ 
    Get the date of the most recent fact in the public.weather_data_raw data
    from postgres and set *most_recent_noaa_data* variable in airflow
    """

    # Get date of most recent data from production table
    postgres = PostgresHook(POSTGRES_STAGING_CONN_ID)
    connection = postgres.get_conn()
    cursor = connection.cursor()
    cursor.execute(SqlQueries.most_recent_noaa_data)
    most_recent = cursor.fetchone()
    try:
        most_recent_day = datetime.strptime(most_recent[0],'%Y%m%d')
    except:
        most_recent_day = NOAA_DATA_AVAILABLE_FROM
    else:
        # Add one day to avoid complications with
        # Dec 31st dates
        most_recent_day += timedelta(days=1)
    print(f'Most recent NOAA data is as of: {most_recent_day}')
    Variable.delete('most_recent_noaa_data')
    Variable.set('most_recent_noaa_data', most_recent_day) #.strftime('%Y%m%d'))

def create_partition_tables() -> None:
    """ 
    Create partition tables for f_climate_data
    """
    start_year = datetime.strptime(NOAA_DATA_AVAILABLE_FROM, '%Y-%m-%d').year
    end_year = datetime.now().year

    postgres = PostgresHook(POSTGRES_STAGING_CONN_ID)
    connection = postgres.get_conn()
    cursor = connection.cursor()
    for year in range(start_year, end_year+1):
        f_sql = SqlQueries.create_partition_table_cmd(
                    schema='public',
                    table='f_climate_data',
                    year=year
                    )
        print(f'Debugging: {f_sql}')
        cursor.execute(f_sql)
    connection.commit()


DAG_NAME = 'noaa_facts_dag'

with DAG(DAG_NAME,
          default_args = DEFAULT_ARGS,
          description = 'Load facts from NOAA S3 bucket into local Postgres',
          catchup = False,
          start_date = DEFAULT_START_DATE,
          concurrency = 4,
          max_active_runs = 4, # to prevent Airflow from running 
                               # multiple days/hours at the same time
          schedule_interval = '0 0 * * *' # run daily at midnight
        ) as dag:

    # execution_date = {{ ds_nodash  }}
    start_operator = DummyOperator(task_id='Begin_execution')

    # Create NOAA fact tables in Staging Database (Postgresql)
    #
    create_noaa_fact_tables_operator = CreateTablesOperator(
        task_id = 'Create_noaa_fact_tables',
        postgres_conn_id = POSTGRES_STAGING_CONN_ID,
        sql_query_file = os.path.join(AIRFLOW_HOME,
                                      POSTGRES_CREATE_FACT_TABLES_FILE),
        )

    # Create partition tables for f_climate_data
    #
    create_partition_tables_operator = PythonOperator(
        task_id = 'Create_partition_tables',
        python_callable = create_partition_tables
        )


    # Get date of most recent NOAA data in the Postgres staging tables
    #
    get_date_of_most_recent_noaa_facts_operator = PythonOperator(
        task_id = 'Get_date_of_most_recent_noaa_facts',
        python_callable = get_date_of_most_recent_noaa_facts
        )

    # Select all facts for date = {{ DS }} from the NOAA S3 bucket
    # and store them as a csv file in the local Staging Area (Filesystem)
    #
    select_noaa_data_from_s3_operator = SelectFromNOAAS3ToStagingOperator(
        task_id = 'Select_noaa_data_from_s3',
        aws_credentials = NOAA_AWS_CREDS,
        s3_bucket = NOAA_S3_BUCKET,
        s3_prefix = NOAA_S3_FACT_PREFIX,
        s3_table_file = '{{ execution_date.year }}.csv.gz',
        most_recent_data_date = '{{ var.value.most_recent_noaa_data }}',
        execution_date = '{{ ds }}',
        real_date = date.today().strftime('%Y-%m-%d'),
        local_path = NOAA_STAGING_FACTS,
        fact_delimiter = NOAA_S3_FACT_DELIM,
        quotation_char = NOAA_QUOTATION_CHAR
        )

    # Load the NOAA fact data for {{ DS }} from csv file on local Staging
    # into the tables prepared on Staging Database (Postgresql)
    #
    load_noaa_fact_tables_into_postgres_operator = LocalStageToPostgresOperator(
        task_id = 'Load_noaa_fact_tables_into_postgres',
        postgres_conn_id = POSTGRES_STAGING_CONN_ID,
        table = 'public.f_weather_data_raw',
        delimiter = NOAA_S3_FACT_DELIM,
        truncate_table = True,
        local_path = NOAA_STAGING_FACTS,
        file_pattern = "*.csv.gz",
        gzipped = True
        )

    # Run quality checks on fact data
    #
    check_fact_quality_operator = DummyOperator(task_id='Check_fact_quality')

    # Finally, insert quality-checked data from Postgres Staging
    # into the "production" database
    #
    transform_noaa_facts_operator = PostgresOperator(
        task_id="transform_noaa_facts",
        postgres_conn_id=POSTGRES_STAGING_CONN_ID,
        sql=SqlQueries.transform_noaa_facts
    )
    end_operator = DummyOperator(task_id='Stop_execution')


# ............................................
# Defining the DAG structure
# ............................................

start_operator >> create_noaa_fact_tables_operator
create_noaa_fact_tables_operator >> create_partition_tables_operator
create_partition_tables_operator >> get_date_of_most_recent_noaa_facts_operator
get_date_of_most_recent_noaa_facts_operator >> select_noaa_data_from_s3_operator
select_noaa_data_from_s3_operator >> load_noaa_fact_tables_into_postgres_operator
load_noaa_fact_tables_into_postgres_operator >> check_fact_quality_operator
check_fact_quality_operator >> transform_noaa_facts_operator
transform_noaa_facts_operator >> end_operator


