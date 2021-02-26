from datetime import date, datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.state import State

from sensors.dag_status import DagStatusSensor

from helpers.sql_queries import SqlQueries

AWS_KEY    = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
AWS_REGION = os.environ.get('AWS_REGION', default='eu-central-1')

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')

## noaa_config needs to be defined when starting Airflow
## Definition resides in ./variables/noaa.json
#
#  Define all required noaa config parameters as constants
#  Access to airflow Variables via "Variables.get" takes up a database
#  connection, thus using the metadata extensively can impact database
#  connectivity.noaa_config: dict = Variable.get("noaa_config", deserialize_json=True)
noaa_config = Variable.get("noaa_config", deserialize_json=True)
NOAA_AWS_CREDS: str = noaa_config['source_params']['aws_credentials']
NOAA_SP_S3_BUCKET: str = noaa_config['source_params']['s3_bucket']
NOAA_SP_S3_KEYS: list = noaa_config['source_params']['s3_keys']
NOAA_SP_S3_FACT_PREFIX: str = noaa_config['source_params']['s3_fact_prefix']
NOAA_SP_S3_FACT_FORMAT: str = noaa_config['source_params']['s3_fact_format']
NOAA_SP_S3_FACT_COMPRESSION: str = noaa_config['source_params']['s3_fact_compression']
NOAA_SP_S3_FACT_DELIM: str  = noaa_config['source_params']['s3_fact_delimiter']
NOAA_DATA_AVAILABLE_FROM: str = noaa_config['data_available_from']
NOAA_STAGING_LOCATION: str = os.path.join(AIRFLOW_HOME, noaa_config['staging_location'])

## Definitions in ./variables/general.json
general_config: dict = Variable.get("general", deserialize_json=True)
CSV_QUOTE_CHAR = general_config['csv_quote_char']
CSV_DELIMITER = general_config['csv_delimiter']
NOAA_STAGING_SCHEMA = general_config['noaa_staging_schema']
PRODUCTION_SCHEMA = general_config['production_schema']

dags_config: dict = Variable.get("dags", deserialize_json=True)
DIM_DAG_NAME   = dags_config['dimension_dag_name']
FACTS_DAG_NAME =  dags_config['facts_dag_name']
PROCESS_DAG_NAME =  dags_config['process_dag_name']


## Start date is yesterday, so that the scheduler starts the task when activated
DEFAULT_START_DATE = datetime.today() - timedelta(days=1)

POSTGRES_STAGING_CONN_ID = os.environ.get('POSTGRES_HOST')
POSTGRES_CREATE_FACT_TABLES_FILE = 'dags/sql/create_facts_tables.sql'


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

with DAG(PROCESS_DAG_NAME,
          default_args = DEFAULT_ARGS,
          description = 'Create simple climate datamart from local Postgres data',
          catchup = False,
          start_date = DEFAULT_START_DATE,
          concurrency = 4,
          max_active_runs = 4, # to prevent Airflow from running
                               # multiple days/hours at the same time
          schedule_interval = '0 0 * * *' # run daily at midnight
        ) as dag:

    # execution_date = {{ ds_nodash  }}
    start_operator = DummyOperator(task_id='Begin_execution')

    # Wait for dimensions and facts data to be updated
    # wait_for_noaa_load_sensor = DagStatusSensor(
    #     task_id='Wait_for_noaa_load',
    #     dag_name = DIM_DAG_NAME,
    #     status_to_check = State.SUCCESS,
    #     poke_interval = 120,
    #     timeout = 3600
    #     )

    # Finally, run reporting and analytics tasks
    # Exemplary, a monthly aggregation of temperature and precipitation is
    # calculated and stored in a separate table
    aggregate_ger_monthly_operator = PostgresOperator(
        task_id="Aggregate_ger_monthly",
        postgres_conn_id=POSTGRES_STAGING_CONN_ID,
        sql=SqlQueries.aggregate_ger_monthly_data
    )

    end_operator = DummyOperator(task_id='Stop_execution')


# ............................................
# Defining the DAG structure
# ............................................

start_operator >> aggregate_ger_monthly_operator
aggregate_ger_monthly_operator >> end_operator
