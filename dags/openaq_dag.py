from datetime import date, datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

## Hooks
from airflow.providers.postgres.hooks.postgres import PostgresHook

## Operators
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from operators.create_tables import CreateTablesOperator
from operators.data_quality import DataQualityOperator
from operators.reformat_fixed_width_file import ReformatFixedWidthFileOperator
from operators.download_openaq_files import DownloadOpenAQFilesOperator
#from operators.select_from_openaq_s3_to_staging import SelectFromOpenAQS3ToStagingOperator
from operators.local_csv_to_postgres import LocalCSVToPostgresOperator
from operators.reformat_openaq_json_files import ReformatOpenAQJSONFilesOperator

from helpers.sql_queries import SqlQueries
from helpers.data_quality_checks import DataQualityChecks


AWS_KEY    = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
AWS_REGION = os.environ.get('AWS_REGION', default='eu-central-1')

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')

## openaq_config needs to be defined when starting Airflow
## Definition resides in ./variables/openaq.json
#
#  Define all required openaq config parameters as constants
#  Access to airflow Variables via "Variables.get" takes up a database
#  connection, thus using the metadata extensively can impact database
#  connectivity.
openaq_config: dict = Variable.get("openaq_config", deserialize_json=True)
OPENAQ_AWS_CREDS: str = openaq_config['source_params']['aws_credentials']
OPENAQ_S3_BUCKET: str = openaq_config['source_params']['s3_bucket']
OPENAQ_S3_KEYS: list = openaq_config['source_params']['s3_keys']
OPENAQ_S3_FACT_DELIM: str  = openaq_config['source_params']['s3_fact_delimiter']
OPENAQ_S3_FACT_PREFIX: str = openaq_config['source_params']['s3_fact_prefix']
OPENAQ_S3_FACT_COMPRESSION: str = openaq_config['source_params']['s3_fact_compression']
OPENAQ_S3_FACT_FORMAT: str = openaq_config['source_params']['s3_fact_format']
OPENAQ_DATA_AVAILABLE_FROM: str = openaq_config['data_available_from']
OPENAQ_STAGING_LOCATION: str = os.path.join(AIRFLOW_HOME, openaq_config['staging_location'])

## Definitions in ./variables/general.json
general_config: dict = Variable.get("general", deserialize_json=True)
CSV_QUOTE_CHAR = general_config['csv_quote_char']
CSV_DELIMITER = general_config['csv_delimiter']
OPENAQ_STAGING_SCHEMA = general_config['openaq_staging_schema']
PRODUCTION_SCHEMA = general_config['production_schema']

dags_config: dict = Variable.get("dags", deserialize_json=True)
OPENAQ_DAG_NAME   = dags_config['openaq_dag_name']
PROCESS_DAG_NAME =  dags_config['process_dag_name']

OPENAQ_STAGING_DIM_RAW = os.path.join(OPENAQ_STAGING_LOCATION,'dimensions_raw')
OPENAQ_STAGING_DIM_CSV = os.path.join(OPENAQ_STAGING_LOCATION,'dimensions_csv')
OPENAQ_STAGING_FACTS = os.path.join(OPENAQ_STAGING_LOCATION,'facts')

## Start date is yesterday, so that the scheduler starts the task when activated
DEFAULT_START_DATE = datetime.today() - timedelta(days=1)

## 'postgres' is the name of the Airflow Connection to the Postgresql
POSTGRES_STAGING_CONN_ID = 'postgres'
POSTGRES_CREATE_NOAA_TABLES_FILE = 'dags/sql/create_noaa_tables.sql'
POSTGRES_CREATE_OPENAQ_TABLES_FILE = 'dags/sql/create_openaq_tables.sql'

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

# def get_date_of_most_recent_openaq_facts() -> None:
#     """
#     Get the date of the most recent fact in the openaq_staging.air_data_raw data
#     from postgres and set *most_recent_openaq_data* variable in airflow
#     """
#
#     # Get date of most recent data from production table
#     postgres = PostgresHook(POSTGRES_STAGING_CONN_ID)
#     connection = postgres.get_conn()
#     cursor = connection.cursor()
#     cursor.execute(SqlQueries.most_recent_openaq_data)
#     most_recent = cursor.fetchone()
#     try:
#         most_recent_day = datetime.strptime(most_recent[0],'%Y%m%d')
#     except:
#         most_recent_day = OPENAQ_DATA_AVAILABLE_FROM
#     else:
#         # Add one day to avoid complications with
#         # Dec 31st dates
#         most_recent_day += timedelta(days=1)
#     print(f'Most recent OPENAQ data is as of: {most_recent_day}')
#     Variable.delete('most_recent_openaq_data')
#     Variable.set('most_recent_openaq_data', most_recent_day) #.strftime('%Y%m%d'))

def create_partition_tables() -> None:
    """
    Create partition tables for f_airpoll_data
    """
    start_year = datetime.strptime(OPENAQ_DATA_AVAILABLE_FROM, '%Y-%m-%d').year
    end_year = datetime.now().year

    postgres = PostgresHook(POSTGRES_STAGING_CONN_ID)
    connection = postgres.get_conn()
    cursor = connection.cursor()
    for year in range(start_year, end_year+1):
        f_sql = SqlQueries.create_partition_table_cmd(
                    schema=f'{PRODUCTION_SCHEMA}',
                    table='f_airpoll_data',
                    year=year
                    )
        cursor.execute(f_sql)
    connection.commit()


with DAG(OPENAQ_DAG_NAME,
          default_args = DEFAULT_ARGS,
          description = 'Load OPENAQ data from OPENAQ S3 to local Postgres',
          catchup = True,
          start_date = DEFAULT_START_DATE,
          concurrency = 8,
          max_active_runs = 8,
          schedule_interval = '0 23 * * *' # run daily at 23pm
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    # -----------------------------------------------------
    #   Create tables in openaq_staging and production schema
    # -----------------------------------------------------
    with TaskGroup("Create_postgres_tables") as create_tables:
        # Create OPENAQ dimension tables in Staging Database (Postgresql)
        create_openaq_tables_operator = CreateTablesOperator(
            task_id = 'Create_openaq_tables',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            sql_query_file = os.path.join(AIRFLOW_HOME,
                                          POSTGRES_CREATE_OPENAQ_TABLES_FILE),
            )
        # Populate d_kpi_names table with most relevant KPI names
        # --> in case this DAG runs before the noaa_dag
        populate_kpi_names_table_operator = PostgresOperator(
            task_id="populate_kpi_names_table",
            postgres_conn_id=POSTGRES_STAGING_CONN_ID,
            sql=SqlQueries.populate_d_kpi_table
        )
        # Create partition tables for f_climate_data
        create_partition_tables_operator = PythonOperator(
            task_id = 'Create_partition_tables',
            python_callable = create_partition_tables
            )
        create_openaq_tables_operator >> populate_kpi_names_table_operator
        create_openaq_tables_operator >> create_partition_tables_operator

    # -----------------------------------------------------
    # Run import of Dimension and Fact data in parallel
    # -----------------------------------------------------
    with TaskGroup("Run_file_import") as run_file_import:
        # Load relevant dimension and documentation files from the
        # OpenAQ S3 bucket into the local Staging Area (Filesystem)
        with TaskGroup("download_openaq_files") as download_openaq_files:
            download_openaq_files_operator = DownloadOpenAQFilesOperator(
                # [:-4]: remove last four characters, i.e. file suffix '.txt'
                task_id = f'Download_openaq_files',
                aws_credentials = OPENAQ_AWS_CREDS,
                s3_bucket = OPENAQ_S3_BUCKET,
                s3_prefix = f"{OPENAQ_S3_FACT_PREFIX}/"+"{{ ds }}",
                local_path = os.path.join(OPENAQ_STAGING_FACTS,
                                          "{{ ds }}")
                )
            ##
            ## Download the CMU station data here!
            ##

        with TaskGroup("reformat_openaq_files") as reformat_openaq_files:
            reformat_openaq_files_operator = ReformatOpenAQJSONFilesOperator(
                task_id = 'Reformat_openaq_files',
                local_path_raw = os.path.join(OPENAQ_STAGING_FACTS,
                                          "{{ ds }}"),
                local_path_csv = os.path.join(OPENAQ_STAGING_FACTS,
                                          "{{ ds }}"),
                delimiter = f'{CSV_DELIMITER}',
                quote = f'{CSV_QUOTE_CHAR}',
                add_header = True,
                file_pattern = '*.ndjson.gz',
                gzipped = True
            )

        with TaskGroup("import_openaq_files") as import_openaq_files:
            import_openaq_files_into_postgres_operator = LocalCSVToPostgresOperator(
                task_id = f'Import_openaq_files_into_postgres',
                postgres_conn_id = POSTGRES_STAGING_CONN_ID,
                table = f'{OPENAQ_STAGING_SCHEMA}.f_air_data_raw',
                delimiter = f'{CSV_DELIMITER}',
                quote = f'{CSV_QUOTE_CHAR}',
                truncate_table = False, #True,
                local_path = os.path.join(OPENAQ_STAGING_FACTS,
                                          "{{ ds }}"),
                file_pattern = f'*.csv',
                gzipped = False
                )

        download_openaq_files >> reformat_openaq_files
        reformat_openaq_files >>  import_openaq_files

        # with TaskGroup("import_openaq_dims") as import_openaq_dims:
        #     s3_index = 2
        #     import_openaq_country_operator = LocalCSVToPostgresOperator(
        #         task_id = f'Import_{OPENAQ_S3_KEYS[s3_index][:-4]}_into_postgres',
        #         postgres_conn_id = POSTGRES_STAGING_CONN_ID,
        #         table = f'{OPENAQ_STAGING_SCHEMA}.ghcnd_countries_raw',
        #         delimiter = f'{CSV_DELIMITER}',
        #         quote = f'{CSV_QUOTE_CHAR}',
        #         truncate_table = True,
        #         local_path = OPENAQ_STAGING_DIM_CSV,
        #         file_pattern = f'{OPENAQ_S3_KEYS[s3_index]}',
        #         gzipped = False
        #         )
        #     s3_index = 3
        #     import_openaq_inventory_operator = LocalStageToPostgresOperator(
        #         task_id = f'Import_{OPENAQ_S3_KEYS[s3_index][:-4]}_into_postgres',
        #         postgres_conn_id = POSTGRES_STAGING_CONN_ID,
        #         table = f'{OPENAQ_STAGING_SCHEMA}.ghcnd_inventory_raw',
        #         delimiter = f'{CSV_DELIMITER}',
        #         quote = f'{CSV_QUOTE_CHAR}',
        #         truncate_table = True,
        #         local_path = OPENAQ_STAGING_DIM_CSV,
        #         file_pattern = f'{OPENAQ_S3_KEYS[s3_index]}',
        #         gzipped = False
        #         )
        #     s3_index = 5
        #     import_openaq_stations_operator = LocalStageToPostgresOperator(
        #         task_id = f'Import_{OPENAQ_S3_KEYS[s3_index][:-4]}_into_postgres',
        #         postgres_conn_id = POSTGRES_STAGING_CONN_ID,
        #         table = f'{OPENAQ_STAGING_SCHEMA}.ghcnd_stations_raw',
        #         delimiter = f'{CSV_DELIMITER}',
        #         quote = f'{CSV_QUOTE_CHAR}',
        #         truncate_table = True,
        #         local_path = OPENAQ_STAGING_DIM_CSV,
        #         file_pattern = f'{OPENAQ_S3_KEYS[s3_index]}',
        #         gzipped = False
        #         )
        #
        # Run quality checks on dimension data
        check_openaq_quality_operator = DataQualityOperator(
            task_id='Check_openaq_quality',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            dq_checks = DataQualityChecks.dq_checks_openaq,
            )

        with TaskGroup("Load_openaq_into_production") as load_openaq_into_production:
            # Transfer raw tables to production tables
            load_openaq_facts_operator = PostgresOperator(
                task_id="load_openaq_facts",
                postgres_conn_id=POSTGRES_STAGING_CONN_ID,
                sql=SqlQueries.load_openaq_facts
            )

        #
        # with TaskGroup("Load_dims_into_production") as load_dims_into_production:
        #     # Transfer raw tables to production tables
        #     load_openaq_countries_operator = PostgresOperator(
        #         task_id="load_openaq_countries",
        #         postgres_conn_id=POSTGRES_STAGING_CONN_ID,
        #         sql=SqlQueries.load_openaq_countries
        #     )
        #
        #     # Transfer raw tables to production tables
        #     load_openaq_stations_operator = PostgresOperator(
        #         task_id="load_openaq_stations",
        #         postgres_conn_id=POSTGRES_STAGING_CONN_ID,
        #         sql=SqlQueries.load_openaq_stations
        #     )
        #
        #     # Transfer raw tables to production tables
        #     load_openaq_inventory_operator = PostgresOperator(
        #         task_id="load_openaq_inventory",
        #         postgres_conn_id=POSTGRES_STAGING_CONN_ID,
        #         sql=SqlQueries.load_openaq_inventory
        #     )
        # ............................................
        # Defining TaskGroup DAG
        # ............................................
        # download_openaq_dims >> reformat_openaq_dims
        # reformat_openaq_dims >> import_openaq_dims
        # import_openaq_dims >> check_dim_quality_operator
        # check_dim_quality_operator >> load_dims_into_production
        #[download_openaq_dims] >> dummy1_operator >> [reformat_openaq_dims]
        #[reformat_openaq_dims] >> dummy2_operator >> [import_openaq_dims]
        #[import_openaq_dims] >> check_dim_quality_operator
        #check_dim_quality_operator >> [load_dims_into_production]

    # -----------------------------------------------------
    # Run import of Dimension and Fact data in parallel
    # -----------------------------------------------------
    # with TaskGroup("Run_fact_import") as run_facts_import:
    #
    #     # Get date of most recent OPENAQ data in the Postgres production tables
    #     get_date_of_most_recent_openaq_facts_operator = PythonOperator(
    #         task_id = 'Get_date_of_most_recent_openaq_facts',
    #         python_callable = get_date_of_most_recent_openaq_facts
    #         )
    #
    #     # Select all facts for date == {{ DS }} from the OPENAQ S3 bucket
    #     # and store them as a csv file in the local Staging Area (Filesystem)
    #     select_openaq_data_from_s3_operator = SelectFromOPENAQS3ToStagingOperator(
    #         task_id = 'Select_openaq_data_from_s3',
    #         aws_credentials = OPENAQ_AWS_CREDS,
    #         s3_bucket = OPENAQ_S3_BUCKET,
    #         s3_prefix = OPENAQ_S3_FACT_PREFIX,
    #         s3_table_file = '{{ execution_date.year }}.csv.gz',
    #         most_recent_data_date = '{{ var.value.most_recent_openaq_data }}',
    #         execution_date = '{{ ds }}',
    #         real_date = date.today().strftime('%Y-%m-%d'),
    #         local_path = OPENAQ_STAGING_FACTS,
    #         fact_delimiter = OPENAQ_S3_FACT_DELIM,
    #         quotation_char = CSV_QUOTE_CHAR
    #         )
    #
    #     # Load the OPENAQ fact data for {{ DS }} from csv file on local Staging
    #     # into the tables prepared on Staging Database (Postgresql)
    #     load_openaq_fact_tables_into_postgres_operator = LocalStageToPostgresOperator(
    #         task_id = 'Load_openaq_fact_tables_into_postgres',
    #         postgres_conn_id = POSTGRES_STAGING_CONN_ID,
    #         table = f'{OPENAQ_STAGING_SCHEMA}.f_weather_data_raw',
    #         delimiter = OPENAQ_S3_FACT_DELIM,
    #         truncate_table = True,
    #         local_path = OPENAQ_STAGING_FACTS,
    #         file_pattern = "*.csv.gz",
    #         gzipped = True
    #         )
    #
    #     # Run quality checks on raw fact data
    #     check_fact_quality_operator = DataQualityOperator(
    #         task_id='Check_fact_quality',
    #         postgres_conn_id = POSTGRES_STAGING_CONN_ID,
    #         dq_checks = DataQualityChecks.dq_checks_facts,
    #         )
    #
    #     # Finally, insert quality-checked data from Postgres Staging
    #     # into the "production" database
    #     transform_openaq_facts_operator = PostgresOperator(
    #         task_id="transform_openaq_facts",
    #         postgres_conn_id=POSTGRES_STAGING_CONN_ID,
    #         sql=SqlQueries.transform_openaq_facts
    #     )
    #     # ............................................
    #     # Defining TaskGroup DAG
    #     # ............................................
    #     get_date_of_most_recent_openaq_facts_operator >> select_openaq_data_from_s3_operator
    #     select_openaq_data_from_s3_operator >> load_openaq_fact_tables_into_postgres_operator
    #     load_openaq_fact_tables_into_postgres_operator >> check_fact_quality_operator
    #     check_fact_quality_operator >> transform_openaq_facts_operator


    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


# ............................................
# Defining the main DAG
# ............................................

start_operator >> create_tables
create_tables >> run_file_import
run_file_import >> end_operator
