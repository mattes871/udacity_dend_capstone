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

# from airflow.operators.subdag import SubDagOperator
# from subdags.download_dim_subdag import download_noaa_dim_subdag
from airflow.utils.task_group import TaskGroup


from operators.create_tables import CreateTablesOperator
from operators.reformat_fixed_width_file import ReformatFixedWidthFileOperator
##from operators.copy_noaa_s3_files_to_staging import CopyNOAAS3FilesToStagingOperator
#from operators.get_s3file_metadata import GetS3FileMetadata
from operators.download_noaa_dim_file_to_staging import DownloadNOAADimFileToStagingOperator

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

DAG_NAME = 'noaa_dimension_dag'

with DAG(DAG_NAME,
          default_args = default_args,
          description = 'Load climate data and create a regular report',
          catchup = False,
          start_date = datetime(year=2021,month=1,day=31),
          concurrency = 4,
          max_active_runs = 4, # to prevent Airflow from running 
                               # multiple days/hours at the same time
          schedule_interval = '@once'
        ) as dag:

    execution_date = "{{ ds_nodash }}"

    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    # Create NOAA dimension tables in Staging Database (Postgresql)
    #
    create_noaa_dim_tables_operator = CreateTablesOperator(
        task_id='Create_noaa_dim_tables',
        postgres_conn_id=postgres_conn_id,
        sql_query_file=os.path.join(os.environ.get('AIRFLOW_HOME','.'),
                                    postgres_create_dim_tables_file),
        dag=dag
        )

    # Load relevant dimension and documentation files from the
    # NOAA S3 bucket into the local Staging Area (Filesystem)
    #
    # >>>> turns out that dynamic SubDAGs in Airflow are still an issue
    # >>>> Unfortunately, static SubDAGs do not improve readability
    #  noaa_dim_s3_to_staging_operator = SubDagOperator(
    #      task_id='Noaa_dim_s3_to_staging',
    #      subdag=download_noaa_dim_subdag(
    #          parent_dag_name=DAG_NAME,
    #          task_id='Noaa_dim_s3_to_staging',
    #          aws_credentials=noaa_aws_creds,
    #          s3_bucket=noaa_s3_bucket,
    #          s3_prefix='',
    #          s3_keys=[noaa_s3_keys[i] for i in [2,3,5]],
    #          replace_existing=True,
    #          local_path=os.path.join(noaa_staging_location, 'dimensions')
    #          ),
    #      dag=dag
    #      )
    # >>>> Using TaskGroup instead of SubDags
    with TaskGroup("download_noaa_dims") as download_noaa_dims:
        s3_index = 2
        load_countries_operator = DownloadNOAADimFileToStagingOperator(
            task_id=f'load_{noaa_s3_keys[s3_index][:-4]}_dim_file',
            aws_credentials=noaa_aws_creds,
            s3_bucket=noaa_s3_bucket,
            s3_prefix='',
            s3_key=noaa_s3_keys[s3_index],
            replace_existing=True,
            local_path=os.path.join(noaa_staging_location, 'dimensions_raw')
            )
        s3_index = 3
        load_inventory_operator = DownloadNOAADimFileToStagingOperator(
            task_id=f'load_{noaa_s3_keys[s3_index][:-4]}_dim_file',
            aws_credentials=noaa_aws_creds,
            s3_bucket=noaa_s3_bucket,
            s3_prefix='',
            s3_key=noaa_s3_keys[s3_index],
            replace_existing=True,
            local_path=os.path.join(noaa_staging_location, 'dimensions_raw')
            )
        s3_index = 5
        load_stations_operator = DownloadNOAADimFileToStagingOperator(
            task_id=f'load_{noaa_s3_keys[s3_index][:-4]}_dim_file',
            aws_credentials=noaa_aws_creds,
            s3_bucket=noaa_s3_bucket,
            s3_prefix='',
            s3_key=noaa_s3_keys[s3_index],
            replace_existing=True,
            local_path=os.path.join(noaa_staging_location, 'dimensions_raw')
            )
        #load_countries >> [load_inventory, load_stations]

    # This dummy_operator is needed to "focus" the *download_noaa_dims* task
    # group and wait for its completion. Without this operator, each of the
    # following operators would be triggered after each of the
    # download_noaa_dims operators, effectively (but wrongly) multiplying the
    # number of tasks to do.
    # 
    dummy_operator = DummyOperator(task_id='Channel_execution', dag=dag)

    # Reformat the fixed-width files into a format that Postgresql can deal with
    #
    with TaskGroup("reformat_noaa_dims") as reformat_noaa_dims:
        s3_index = 2
        reformat_countries_operator = ReformatFixedWidthFileOperator(
            task_id=f'reformat_{noaa_s3_keys[s3_index][:-4]}_dim_file',
            filename=noaa_s3_keys[s3_index],
            local_path_fixed_width=os.path.join(noaa_staging_location, 'dimensions_raw'),
            local_path_csv=os.path.join(noaa_staging_location, 'dimensions'),
            column_names=['country_id', 'country'],
            column_positions=[0, 3],
            delimiter='|',
            add_header=True,
            remove_original_file=False,
            )

        s3_index = 3
        reformat_inventory_operator = ReformatFixedWidthFileOperator(
            task_id=f'reformat_{noaa_s3_keys[s3_index][:-4]}_dim_file',
            filename=noaa_s3_keys[s3_index],
            local_path_fixed_width=os.path.join(noaa_staging_location, 'dimensions_raw'),
            local_path_csv=os.path.join(noaa_staging_location, 'dimensions'),
            column_names=['id', 'latitude','longitude','kpi','from_year','until_year'],
            column_positions=[0, 11, 20, 30, 35, 40, 45],
            delimiter='|',
            add_header=True,
            remove_original_file=False,
            )

        s3_index = 5
        reformat_stations_operator = ReformatFixedWidthFileOperator(
            task_id=f'reformat_{noaa_s3_keys[s3_index][:-4]}_dim_file',
            filename=noaa_s3_keys[s3_index],
            local_path_fixed_width=os.path.join(noaa_staging_location, 'dimensions_raw'),
            local_path_csv=os.path.join(noaa_staging_location, 'dimensions'),
            column_names=['id','latitude','longitude','elevation',
                          'state','name','gsn_flag','hcn_crn_flag','wmo_id'],
            column_positions=[0, 11, 20, 30, 37, 40, 71, 75, 79, 85],
            delimiter='|',
            add_header=True,
            remove_original_file=False,
            )
        #[reformat_countries_operator, reformat_inventory_operator] >> reformat_stations_operator

    # In case the data changed, load the NOAA dimension data from csv file on
    # local Staging into the tables prepared on Staging Database (Postgresql)
    #
    load_noaa_dim_tables_into_postgres_operator = DummyOperator( # LocalStageToPostgresOperator( 
        task_id='Load_noaa_dim_tables_into_postgres',
        #  postgres_conn_id=postgres_conn_id,
        #  table='public.weather_data_raw',
        #  delimiter=',',
        #  truncate_table=True,
        #  local_path=os.path.join(noaa_staging_location,'facts'),
        dag=dag
        )

    # Run quality checks on dimension data
    #
    check_dim_quality_operator = DummyOperator(
        task_id='Check_dim_quality',
        dag=dag
    )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)





# ............................................
# Defining the DAG
# ............................................

start_operator >> create_noaa_dim_tables_operator
create_noaa_dim_tables_operator >> [download_noaa_dims] 
[download_noaa_dims] >> dummy_operator >> [reformat_noaa_dims] 
[reformat_noaa_dims] >> load_noaa_dim_tables_into_postgres_operator
load_noaa_dim_tables_into_postgres_operator >> check_dim_quality_operator
check_dim_quality_operator >> end_operator


