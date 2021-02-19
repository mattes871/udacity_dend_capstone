from datetime import date, datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator

from operators.create_tables import CreateTablesOperator
from operators.copy_noaa_s3_files_to_staging import CopyNOAAS3FilesToStagingOperator
from operators.select_from_noaa_s3_to_staging import SelectFromNOAAS3ToStagingOperator
from operators.local_stage_to_postgres import LocalStageToPostgresOperator

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

#import sh
import subprocess

def get_s3file_metadata(**kwargs: dict) -> any:
    """ Use AWS CLI to get metadata of certain S3 files
        Unfortunately, metadata functionality is not (yet)
        built into the aws hooks and operators
        AWSCLI must be installed
        Requires params:
        bucket: S3 bucket name
        prefix: S3 prefix string
        keys: list of S3 key names
    """
    bucket = "{{ noaa_bucket }}"
    prefix = ''
    keys = "{{ noaa_keys }}"

    print(f"XXXXXXX: {kwargs}")
    results = []
    for key in keys:
        cmd=f"aws s3api head-object --bucket '{bucket}' --key '{key}'"
        print(f"YYYYY {{ noaa_bucket }} {kwargs['bucket']}")
        #cmd=["aws","s3api", "head-object", f"--bucket '{bucket}'" f"--key '{key}'"]
        #cmd = ["aws s3api head-object --bucket 'noaa-ghcn-pds --key 'ghcnd-stations.txt'"]
        try:
            result=subprocess.check_output(cmd)
        except:
            result=f'Could not execute {cmd}'
        else:
            results.append(result)
        #result=subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE)
        #std_out,err = result.communicate()
        print(f'------------------------- aws s3api head-object(): {result}')

    #  s3 = sh.bash.bake(f"aws s3api head-object --bucket '{bucket}' --key '{key}'")
    #  result = s3
    
    #  s3_hook = S3Hook('aws_credentials')
    #  result =
    #  result = s3_hook.list_keys(
    #              bucket_name=bucket,
    #              delimiter='/'
    #              )
    # print(f'------------------------- aws s3api head-object(): {result}')



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
    get_noaa_files_metadata_operator = PythonOperator(
        task_id='Get_noaa_files_metadata',
        provide_context=True,
        python_callable=get_s3file_metadata,
        op_kwargs={'bucket': f"{{ noaa_bucket }}",
                   'prefix': '',
                   'keys': f"{{ noaa_files }}"}
        )
        # 'bucket'=f"{noaa.source_params['s3_bucket']}",

    # Load relevant dimension and documentation files from the
    # NOAA S3 bucket into the local Staging Area (Filesystem)
    #
    copy_noaa_s3_files_to_staging_operator = DummyOperator(task_id='dummy1')
    #  CopyNOAAS3FilesToStagingOperator(
    #      task_id='Copy_noaa_s3_files_to_staging',
    #      aws_credentials=noaa.source_params['aws_credentials'],
    #      s3_bucket=noaa.source_params['s3_bucket'],
    #      s3_prefix='',
    #      s3_files=noaa.source_params['files'],
    #      replace_existing=True,
    #      local_path=os.path.join(noaa.staging_location,'dimensions')
    #      )

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


