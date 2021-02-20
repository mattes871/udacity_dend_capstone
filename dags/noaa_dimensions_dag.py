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
from operators.copy_noaa_s3_files_to_staging import CopyNOAAS3FilesToStagingOperator
from operators.select_from_noaa_s3_to_staging import SelectFromNOAAS3ToStagingOperator
from operators.local_stage_to_postgres import LocalStageToPostgresOperator
from operators.get_s3file_metadata import GetS3FileMetadata


from helpers.sql_queries import SqlQueries
from helpers.source_data_class import SourceDataClass

AWS_KEY    = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
AWS_REGION = os.environ.get('AWS_REGION', default='eu-central-1')

noaa = SourceDataClass(
    source_name='noaa',
    description="""Climate KPIs from 200k stations worldwide, 
                    dating back as far as 1763""",
    source_type='amazon s3',
    source_params={
        'aws_credentials': 'aws_credentials',
        's3_bucket': 'noaa-ghcn-pds',
        'files':  ['by-year-status.txt',
                   'ghcn-daily-by_year-format.rtf',
                   #  'ghcnd-countries.txt',
                   #  'ghcnd-inventory.txt',
                   #  'ghcnd-states.txt',
                   #  'ghcnd-stations.txt',
                   #  'ghcnd-version.txt',
                   #  'index.html',
                   #  'mingle-list.txt',
                   'readme.txt',
                   'status.txt'],
        'prefixes': ['csv','csv.gz'],
        'fact_format': 'csv',
        'compression': 'gzip',
        'delim':       ','},
    data_available_from=date(year=2019,month=1,day=1),
    staging_location='./staging_files/noaa',
    version='v2021-02-05')


Variable.delete('noaa_test')
Variable.set('noaa_test', True)

Variable.delete('noaa_bucket')
Variable.set('noaa_bucket', noaa.source_params['s3_bucket'])
Variable.delete('noaa_files')
Variable.set('noaa_files', noaa.source_params['files'])




default_start_date = datetime(year=2021,month=1,day=31)

postgres_conn_id = 'postgres'
postgres_create_dim_tables_file = './dags/sql/create_dim_tables.sql'
csv_delimiter = ','

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
        sql_query_file=postgres_create_dim_tables_file
        )

    # Get metadata for dim- and doc-files listed in
    # the noaa data class, especially the LastModified info
    #
    get_noaa_files_metadata_operator = GetS3FileMetadata(
        task_id='Get_noaa_files_metadata',
        s3_bucket = noaa.source_params['s3_bucket'], #"{{ noaa_bucket }}",
        s3_prefix = '',
        s3_keys   = noaa.source_params['files'] #,"{{ noaa_files }}"
        )

    # Load relevant dimension and documentation files from the
    # NOAA S3 bucket into the local Staging Area (Filesystem)
    #
    copy_noaa_s3_files_to_staging_operator = CopyNOAAS3FilesToStagingOperator(
        task_id='Copy_noaa_s3_files_to_staging',
        aws_credentials=noaa.source_params['aws_credentials'],
        s3_bucket=noaa.source_params['s3_bucket'],
        s3_prefix='',
        s3_files=noaa.source_params['files'],
        replace_existing=True,
        local_path=os.path.join(noaa.staging_location,'dimensions')
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
        #  local_path=os.path.join(noaa.staging_location,'facts'),
        #  )

    # Run quality checks on dimension data
    #
    check_dim_quality_operator = DummyOperator(task_id='Check_dim_quality')

    end_operator = DummyOperator(task_id='Stop_execution')


# ............................................
# Defining the DAG
# ............................................

start_operator >> create_noaa_dim_tables_operator

create_noaa_dim_tables_operator >> get_noaa_files_metadata_operator
get_noaa_files_metadata_operator >> copy_noaa_s3_files_to_staging_operator
copy_noaa_s3_files_to_staging_operator >> load_noaa_dim_tables_into_postgres_operator
load_noaa_dim_tables_into_postgres_operator >> check_dim_quality_operator
check_dim_quality_operator >> end_operator


