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
# from operators.create_tables import CreateTablesOperator

from helpers.sql_queries import SqlQueries

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

NOAA_QUOTATION_CHAR = '"'

DEFAULT_START_DATE = datetime.now()

## 'postgres' is the name of the Airflow Connection to the Postgresql 
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

DIM_DAG_NAME   = 'noaa_dimension_dag'
FACTS_DAG_NAME = 'noaa_facts_dag'
DAG_NAME = 'process_dims_and_facts_dag'

with DAG(DAG_NAME,
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
    #
    wait_for_dim_data_sensor = DagStatusSensor(
        task_id = 'wait_for_dim_data',
        dag_name = DIM_DAG_NAME,
        status_to_check = State.SUCCESS
        )

    # Wait for dimensions and facts data to be updated
    #
    wait_for_facts_data_sensor = DagStatusSensor(
        task_id = 'wait_for_facts_data',
        dag_name = FACTS_DAG_NAME,
        status_to_check = State.SUCCESS
        )

    # Finally, run reporting and analytics tasks
    # Exemplary, a monthly aggregation of temperature and precipitation is
    # calculated and stored in a separate table
    #
    aggregate_ger_monthly_operator = PostgresOperator(
        task_id="Aggregate_ger_monthly",
        postgres_conn_id=POSTGRES_STAGING_CONN_ID,
        sql=SqlQueries.aggregate_ger_monthly_data
    )

    end_operator = DummyOperator(task_id='Stop_execution')


# ............................................
# Defining the DAG structure
# ............................................

start_operator >> [wait_for_dim_data_sensor, wait_for_facts_data_sensor]
[wait_for_dim_data_sensor, wait_for_facts_data_sensor] >> aggregate_ger_monthly_operator
aggregate_ger_monthly_operator >> end_operator

