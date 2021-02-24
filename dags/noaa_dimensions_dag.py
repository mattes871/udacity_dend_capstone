from datetime import date, datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from hooks.s3_hook_local import S3HookLocal

# from airflow.operators.subdag import SubDagOperator
# from subdags.download_dim_subdag import download_noaa_dim_subdag
from airflow.utils.task_group import TaskGroup


from operators.create_tables import CreateTablesOperator
from operators.reformat_fixed_width_file import ReformatFixedWidthFileOperator
from operators.download_noaa_dim_file_to_staging import DownloadNOAADimFileToStagingOperator
#from operators.download_s3_file_to_staging import DownloadS3FileToStagingOperator
from operators.select_from_noaa_s3_to_staging import SelectFromNOAAS3ToStagingOperator
from operators.local_stage_to_postgres import LocalStageToPostgresOperator


# from helpers.sql_queries import SqlQueries
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
noaa_config = Variable.get("noaa_config", deserialize_json=True)
NOAA_AWS_CREDS = noaa_config['source_params']['aws_credentials']
NOAA_S3_BUCKET = noaa_config['source_params']['s3_bucket']
NOAA_S3_KEYS   = noaa_config['source_params']['s3_keys']
NOAA_S3_FACT_DELIM  = noaa_config['source_params']['fact_delimiter']
NOAA_DATA_AVAILABLE_FROM = noaa_config['data_available_from']
NOAA_STAGING_LOCATION = os.path.join(AIRFLOW_HOME, noaa_config['staging_location'])

NOAA_S3_DIM_DELIM = '|'
NOAA_STAGING_DIM_RAW = os.path.join(NOAA_STAGING_LOCATION,'dimensions_raw')
NOAA_STAGING_DIM_CSV = os.path.join(NOAA_STAGING_LOCATION,'dimensions_csv')
NOAA_QUOTATION_CHAR = '"'

default_start_date = datetime(year=2021,month=1,day=31)

## 'postgres' is the name of the Airflow Connection to the Postgresql 
POSTGRES_STAGING_CONN_ID = os.environ.get('POSTGRES_HOST')
POSTGRES_CREATE_DIM_TABLES_FILE = 'dags/sql/create_dim_tables.sql'

print(f"""Environment for noaa_dimensions_dag:
NOAA_AWS_CREDS: {NOAA_AWS_CREDS}
NOAA_S3_BUCKET: {NOAA_S3_BUCKET}
NOAA_S3_KEYS: {NOAA_S3_KEYS}
NOAA_S3_FACT_DELIM: {NOAA_S3_FACT_DELIM} 
NOAA_DATA_AVAILABLE_FROM: {NOAA_DATA_AVAILABLE_FROM} 
NOAA_STAGING_LOCATION: {NOAA_STAGING_LOCATION}
NOAA_S3_DIM_DELIM: {NOAA_S3_DIM_DELIM}
NOAA_STAGING_DIM_RAW: {NOAA_STAGING_DIM_RAW}
NOAA_STAGING_DIM_CSV: {NOAA_STAGING_DIM_CSV}
NOAA_QUOTATION_CHAR: {NOAA_QUOTATION_CHAR}
POSTGRES_STAGING_CONN_ID: {POSTGRES_STAGING_CONN_ID}
POSTGRES_CREATE_DIM_TABLES_FILE: {POSTGRES_CREATE_DIM_TABLES_FILE} 
""")

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
          schedule_interval = '0 0 * * *' # run daily at midnight
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    # Create NOAA dimension tables in Staging Database (Postgresql)
    #
    create_noaa_dim_tables_operator = CreateTablesOperator(
        task_id = 'Create_noaa_dim_tables',
        postgres_conn_id = POSTGRES_STAGING_CONN_ID,
        sql_query_file = os.path.join(AIRFLOW_HOME,
                                      POSTGRES_CREATE_DIM_TABLES_FILE),
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
    #          aws_credentials=NOAA_AWS_CREDS,
    #          s3_bucket=NOAA_S3_BUCKET,
    #          s3_prefix='',
    #          s3_keys=[NOAA_S3_KEYS[i] for i in [2,3,5]],
    #          replace_existing=True,
    #          local_path=os.path.join(noaa_staging_location, 'dimensions')
    #          ),
    #      )
    # >>>> Using TaskGroup instead of SubDags
    with TaskGroup("download_noaa_dims") as download_noaa_dims:
        s3_index = 2
        download_noaa_countries_operator = DownloadNOAADimFileToStagingOperator(
            # [:-4]: remove last four characters, i.e. file suffix '.txt'
            task_id = f'Download_{NOAA_S3_KEYS[s3_index][:-4]}_dim_file',
            aws_credentials = NOAA_AWS_CREDS,
            s3_bucket = NOAA_S3_BUCKET,
            s3_prefix = '',
            s3_key = NOAA_S3_KEYS[s3_index],
            replace_existing = True,
            local_path = NOAA_STAGING_DIM_RAW
            )
        s3_index = 3
        download_noaa_inventory_operator = DownloadNOAADimFileToStagingOperator(
            task_id = f'Download_{NOAA_S3_KEYS[s3_index][:-4]}_dim_file',
            aws_credentials = NOAA_AWS_CREDS,
            s3_bucket = NOAA_S3_BUCKET,
            s3_prefix = '',
            s3_key = NOAA_S3_KEYS[s3_index],
            replace_existing = True,
            local_path = NOAA_STAGING_DIM_RAW
            )
        s3_index = 5
        download_noaa_stations_operator = DownloadNOAADimFileToStagingOperator(
            task_id = f'Download_{NOAA_S3_KEYS[s3_index][:-4]}_dim_file',
            aws_credentials = NOAA_AWS_CREDS,
            s3_bucket = NOAA_S3_BUCKET,
            s3_prefix = '',
            s3_key = NOAA_S3_KEYS[s3_index],
            replace_existing = True,
            local_path = NOAA_STAGING_DIM_RAW
            )
        #load_countries >> [load_inventory, load_stations]

    # This dummy_operator is needed between Task Groups. Without this operator,
    # each of the following operators would be triggered after each of the
    # download_noaa_dims operators, effectively (but wrongly) multiplying the
    # number of tasks to do.
    # 
    dummy1_operator = DummyOperator(task_id='Buffer_operator_for_task_groups_1', dag=dag)

    # Reformat the fixed-width files into a format that Postgresql can deal with
    #
    with TaskGroup("reformat_noaa_dims") as reformat_noaa_dims:
        s3_index = 2
        reformat_noaa_countries_operator = ReformatFixedWidthFileOperator(
            task_id = f'Reformat_{NOAA_S3_KEYS[s3_index][:-4]}_dim_file',
            filename = NOAA_S3_KEYS[s3_index],
            local_path_fixed_width = NOAA_STAGING_DIM_RAW, 
            local_path_csv = NOAA_STAGING_DIM_CSV,
            column_names = ['country_id', 'country'],
            column_positions = [0, 3],
            delimiter = f'{NOAA_S3_DIM_DELIM}',
            quote = f'{NOAA_QUOTATION_CHAR}',
            add_header = True,
            remove_original_file = False,
            )

        s3_index = 3
        reformat_noaa_inventory_operator = ReformatFixedWidthFileOperator(
            task_id = f'Reformat_{NOAA_S3_KEYS[s3_index][:-4]}_dim_file',
            filename = NOAA_S3_KEYS[s3_index],
            local_path_fixed_width = NOAA_STAGING_DIM_RAW,
            local_path_csv = NOAA_STAGING_DIM_CSV,
            column_names = ['id', 'latitude','longitude','kpi','from_year','until_year'],
            column_positions = [0, 11, 20, 30, 35, 40],
            delimiter = f'{NOAA_S3_DIM_DELIM}',
            quote = f'{NOAA_QUOTATION_CHAR}',
            add_header = True,
            remove_original_file = False,
            )

        s3_index = 5
        reformat_noaa_stations_operator = ReformatFixedWidthFileOperator(
            task_id = f'Reformat_{NOAA_S3_KEYS[s3_index][:-4]}_dim_file',
            filename = NOAA_S3_KEYS[s3_index],
            local_path_fixed_width = NOAA_STAGING_DIM_RAW,
            local_path_csv = NOAA_STAGING_DIM_CSV,
            column_names = ['id','latitude','longitude','elevation',
                          'state','name','gsn_flag','hcn_crn_flag','wmo_id'],
            column_positions = [0, 11, 20, 30, 37, 40, 71, 75, 79],
            delimiter = f'{NOAA_S3_DIM_DELIM}',
            quote = f'{NOAA_QUOTATION_CHAR}',
            add_header = True,
            remove_original_file = False,
            )
        #[reformat_countries_operator, reformat_inventory_operator] >> reformat_stations_operator


    dummy2_operator = DummyOperator(task_id='Buffer_operator_for_task_groups_2', dag=dag)


    # In case the data changed, load the NOAA dimension data from csv file on
    # local Staging into the tables prepared on Staging Database (Postgresql)
    #
    # Currently, there is no mechanism to stop the import in case the staging
    # files have not changed. Dimension data is hence reloaded every time the
    # DAG is executed.
    #
    with TaskGroup("import_noaa_dims") as import_noaa_dims:
        s3_index = 2
        import_noaa_country_operator = LocalStageToPostgresOperator( 
            task_id = f'Import_{NOAA_S3_KEYS[s3_index][:-4]}_into_postgres',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            table = 'public.ghcnd_countries_raw',
            delimiter = f'{NOAA_S3_DIM_DELIM}',
            quote = f'{NOAA_QUOTATION_CHAR}',
            truncate_table = True,
            local_path = NOAA_STAGING_DIM_CSV,
            file_pattern = f'{NOAA_S3_KEYS[s3_index]}',
            gzipped = False
            )
        s3_index = 3
        import_noaa_inventory_operator = LocalStageToPostgresOperator( 
            task_id = f'Import_{NOAA_S3_KEYS[s3_index][:-4]}_into_postgres',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            table = 'public.ghcnd_inventory_raw',
            delimiter = f'{NOAA_S3_DIM_DELIM}',
            quote = f'{NOAA_QUOTATION_CHAR}',
            truncate_table = True,
            local_path = NOAA_STAGING_DIM_CSV,
            file_pattern = f'{NOAA_S3_KEYS[s3_index]}',
            gzipped = False
            )
        s3_index = 5 
        import_noaa_stations_operator = LocalStageToPostgresOperator( 
            task_id = f'Import_{NOAA_S3_KEYS[s3_index][:-4]}_into_postgres',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            table = 'public.ghcnd_stations_raw',
            delimiter = f'{NOAA_S3_DIM_DELIM}',
            quote = f'{NOAA_QUOTATION_CHAR}',
            truncate_table = True,
            local_path = NOAA_STAGING_DIM_CSV,
            file_pattern = f'{NOAA_S3_KEYS[s3_index]}',
            gzipped = False
            )

    # Add Index to tables in order to speed up quality checks as well as later
    # transformation and joins
    #
    add_index_to_dim_tables_operator = DummyOperator(
        task_id = 'Add_index_to_dim_tables'
        )


    # Run quality checks on dimension data
    #
    check_dim_quality_operator = DummyOperator(
        task_id = 'Check_dim_quality'
    )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)





# ............................................
# Defining the DAG
# ............................................

start_operator >> create_noaa_dim_tables_operator
create_noaa_dim_tables_operator >> [download_noaa_dims] 
[download_noaa_dims] >> dummy1_operator >> [reformat_noaa_dims] 
[reformat_noaa_dims] >> dummy2_operator >> [import_noaa_dims]
[import_noaa_dims] >> add_index_to_dim_tables_operator
add_index_to_dim_tables_operator >> check_dim_quality_operator
check_dim_quality_operator >> end_operator


