from datetime import date, datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from hooks.s3_hook_local import S3HookLocal
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator

from operators.create_tables import CreateTablesOperator
#from operators.copy_noaa_s3_files_to_staging import CopyNOAAS3FilesToStagingOperator
#from operators.get_s3file_metadata import GetS3FileMetadata
from operators.copy_noaa_dim_file_to_staging import CopyNOAADimFileToStagingOperator

from operators.select_from_noaa_s3_to_staging import SelectFromNOAAS3ToStagingOperator
from operators.local_stage_to_postgres import LocalStageToPostgresOperator


from helpers.sql_queries import SqlQueries
from helpers.source_data_class import SourceDataClass

AWS_KEY    = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
AWS_REGION = os.environ.get('AWS_REGION', default='eu-central-1')

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')

noaa_config = Variable.get("noaa_config", deserialize_json=True)
noaa_aws_creds = noaa_config['source_params']['aws_credentials']
noaa_s3_bucket = noaa_config['source_params']['s3_bucket']
noaa_s3_keys   = noaa_config['source_params']['s3_keys']
noaa_s3_delim  = noaa_config['source_params']["delimiter"]
noaa_data_available_from = noaa_config['data_available_from']
noaa_staging_location = os.path.join(AIRFLOW_HOME, noaa_config['staging_location'])


default_start_date = datetime(year=2021,month=1,day=31)

postgres_conn_id = 'postgres'
postgres_create_dim_tables_file = 'dags/sql/create_dim_tables.sql'

default_args = {
    'owner': 'matkir',
    'depends_on_past': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': default_start_date,
    'region': AWS_REGION
}


with DAG('noaa_dimension_dag',
          default_args = default_args,
          description = 'Load climate data and create a regular report',
          catchup = False,
          start_date = datetime(year=2021,month=1,day=31),
          concurrency = 4,
          max_active_runs = 4, # to prevent Airflow from running 
                               # multiple days/hours at the same time
          schedule_interval = None
        ) as dag:

    execution_date = "{{ ds_nodash }}"

    start_operator = DummyOperator(task_id='Begin_execution')

    # Create NOAA dimension tables in Staging Database (Postgresql)
    #
    create_noaa_dim_tables_operator = CreateTablesOperator(
        task_id='Create_noaa_dim_tables',
        postgres_conn_id=postgres_conn_id,
        sql_query_file=os.path.join(os.environ.get('AIRFLOW_HOME','.'),
                                    postgres_create_dim_tables_file)
        )

    # Load relevant dimension and documentation files from the
    # NOAA S3 bucket into the local Staging Area (Filesystem)
    #
    copy_noaa_dim_file_to_staging_operator = CopyNOAADimFileToStagingOperator(
        task_id='Copy_noaa_dim_file_to_staging',
        aws_credentials=noaa_aws_creds,
        s3_bucket=noaa_s3_bucket,
        s3_prefix='',
        s3_key=noaa_s3_keys[0],
        replace_existing=True,
        local_path=os.path.join(noaa_staging_location, 'dimensions')
        )

    # In case the data changed, load the NOAA dimension data from csv file on
    # local Staging into the tables prepared on Staging Database (Postgresql)
    #
    load_noaa_dim_tables_into_postgres_operator = DummyOperator(
        task_id='Load_noaa_dim_tables_into_postgres')
        #  LocalStageToPostgresOperator(
        #  task_id='Load_noaa_dim_tables_into_postgres',
        #  postgres_conn_id=postgres_conn_id,
        #  table='public.weather_data_raw',
        #  delimiter=',',
        #  truncate_table=True,
        #  local_path=os.path.join(noaa_staging_location,'facts'),
        #  )

    # Run quality checks on dimension data
    #
    check_dim_quality_operator = DummyOperator(task_id='Check_dim_quality')

    end_operator = DummyOperator(task_id='Stop_execution')


# ............................................
# Defining the DAG
# ............................................

start_operator >> create_noaa_dim_tables_operator

create_noaa_dim_tables_operator >> copy_noaa_dim_file_to_staging_operator
copy_noaa_dim_file_to_staging_operator >> load_noaa_dim_tables_into_postgres_operator
load_noaa_dim_tables_into_postgres_operator >> check_dim_quality_operator
check_dim_quality_operator >> end_operator


