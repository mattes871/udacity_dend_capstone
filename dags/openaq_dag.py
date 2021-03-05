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
from operators.completeness_check_files_vs_postgres import CompletenessCheckFilesVsPostgresOperator
from operators.reformat_fixed_width_file import ReformatFixedWidthFileOperator
from operators.local_csv_to_postgres import LocalCSVToPostgresOperator
from operators.openaq.download_openaq_files import DownloadOpenAQFilesOperator
from operators.openaq.reformat_openaq_json_files import ReformatOpenAQJSONFilesOperator

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
NOAA_DAG_NAME   = dags_config['noaa_dag_name']
OPENAQ_DAG_NAME =  dags_config['openaq_dag_name']
PROCESS_DAG_NAME =  dags_config['process_dag_name']

## 'postgres' is the name of the Airflow Connection to the Postgresql
POSTGRES_STAGING_CONN_ID = 'postgres'
POSTGRES_CREATE_NOAA_TABLES_FILE = 'dags/sql/create_noaa_tables.sql'
POSTGRES_CREATE_OPENAQ_TABLES_FILE = 'dags/sql/create_openaq_tables.sql'
POSTGRES_CREATE_COMMON_TABLES_FILE = 'dags/sql/create_common_tables.sql'

OPENAQ_STAGING_DIM_RAW = os.path.join(OPENAQ_STAGING_LOCATION,'dimensions_raw')
OPENAQ_STAGING_DIM_CSV = os.path.join(OPENAQ_STAGING_LOCATION,'dimensions_csv')
OPENAQ_STAGING_FACTS = os.path.join(OPENAQ_STAGING_LOCATION,'facts')

## Start date is yesterday, so that the scheduler starts the task when activated
# DEFAULT_START_DATE = datetime.today() - timedelta(days=1)

## Start with the earliest available data
DEFAULT_START_DATE = datetime.strptime(OPENAQ_DATA_AVAILABLE_FROM,'%Y-%m-%d')


DEFAULT_ARGS = {
    'owner': 'matkir',
    # ensure that DAG is run chronologically, and terminates before next
    # DAG for following execution_date is run
    'depends_on_past': True,
    'retries': 3,
    'catchup': True,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': DEFAULT_START_DATE,
    'region': AWS_REGION
}

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
                    table='f_climate_data',
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
        # Create Common fact tables in Production Database (Postgresql)
        create_common_tables_operator = CreateTablesOperator(
            task_id = 'Create_common_tables',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            sql_query_file = os.path.join(AIRFLOW_HOME,
                                          POSTGRES_CREATE_COMMON_TABLES_FILE),
            )
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
            sql=SqlQueries.openaq_populate_d_kpi_table
        )
        # Create partition tables for f_climate_data
        create_partition_tables_operator = PythonOperator(
            task_id = 'Create_partition_tables',
            python_callable = create_partition_tables
            )
        create_common_tables_operator >> create_openaq_tables_operator
        create_openaq_tables_operator >> populate_kpi_names_table_operator
        create_openaq_tables_operator >> create_partition_tables_operator

    # -----------------------------------------------------
    # Run import of Dimension and Fact data in parallel
    # -----------------------------------------------------
    with TaskGroup("Run_file_import") as run_file_import:
        # Download json-files with daily / intra-daily data from
        # OpenAQ S3 bucket into the local Staging Area (Filesystem)
        with TaskGroup("download_openaq_files") as download_openaq_files:
            download_openaq_files_operator = DownloadOpenAQFilesOperator(
                task_id = f'Download_openaq_files',
                aws_credentials = OPENAQ_AWS_CREDS,
                s3_bucket = OPENAQ_S3_BUCKET,
                s3_prefix = f"{OPENAQ_S3_FACT_PREFIX}/"+"{{ yesterday_ds }}",
                local_path = os.path.join(OPENAQ_STAGING_FACTS,
                                          "{{ yesterday_ds }}")
                )

        # Convert .ndjson.gz-Files to CSV / OpenAQ-specific processing
        with TaskGroup("reformat_openaq_files") as reformat_openaq_files:
            reformat_openaq_files_operator = ReformatOpenAQJSONFilesOperator(
                task_id = 'Reformat_openaq_files',
                local_path_raw = os.path.join(OPENAQ_STAGING_FACTS,
                                          "{{ yesterday_ds }}"),
                local_path_csv = os.path.join(OPENAQ_STAGING_FACTS,
                                          "{{ yesterday_ds }}"),
                delimiter = f'{CSV_DELIMITER}',
                quote = f'{CSV_QUOTE_CHAR}',
                add_header = True,
                file_pattern = '*.ndjson.gz',
                gzipped = True
            )

        # Load data from CSV files into Postgresql
        with TaskGroup("import_openaq_files") as import_openaq_files:
            import_openaq_files_into_postgres_operator = LocalCSVToPostgresOperator(
                task_id = f'Import_openaq_files_into_postgres',
                postgres_conn_id = POSTGRES_STAGING_CONN_ID,
                table = f'{OPENAQ_STAGING_SCHEMA}.f_air_data_raw',
                delimiter = f'{CSV_DELIMITER}',
                quote = f'{CSV_QUOTE_CHAR}',
                truncate_table = True,
                local_path = os.path.join(OPENAQ_STAGING_FACTS,
                                          "{{ yesterday_ds }}"),
                file_pattern = f'*.csv',
                gzipped = False
                )

        download_openaq_files >> reformat_openaq_files
        reformat_openaq_files >>  import_openaq_files

        with TaskGroup("check_openaq_dataquality") as check_openaq_dataquality:
            # Run quality checks on fact data
            completeness_check_openaq_operator = CompletenessCheckFilesVsPostgresOperator(
                task_id = 'Completeness_check_openaq',
                schema = f'{OPENAQ_STAGING_SCHEMA}',
                table = 'f_air_data_raw',
                local_path = os.path.join(OPENAQ_STAGING_FACTS,
                                          "{{ yesterday_ds }}"),
                file_pattern = '*.csv',
                header_line = True # The reformatted csv files do have a header line
                )
            # Run quality checks on dimension data
            check_openaq_quality_operator = DataQualityOperator(
                task_id = 'Check_openaq_quality',
                postgres_conn_id = POSTGRES_STAGING_CONN_ID,
                dq_checks = DataQualityChecks.dq_checks_openaq,
                )

        with TaskGroup("Load_openaq_into_production") as load_openaq_into_production:
            # Transfer raw tables to production tables
            load_openaq_stations_operator = PostgresOperator(
                task_id="load_openaq_stations",
                postgres_conn_id=POSTGRES_STAGING_CONN_ID,
                sql=SqlQueries.load_openaq_stations
            )
            # Transfer raw tables to production tables
            load_openaq_facts_operator = PostgresOperator(
                task_id="load_openaq_facts",
                postgres_conn_id=POSTGRES_STAGING_CONN_ID,
                sql=SqlQueries.transform_openaq_avg_facts
            )
            ## Tasks for later implementation:
            #  - Update d_country and d_inventory tables
            #  - Insert further aggregate-KPIs, e.g. min/max values

            load_openaq_stations_operator >> load_openaq_facts_operator

        import_openaq_files >> check_openaq_dataquality
        check_openaq_dataquality >> load_openaq_into_production

    # After successful transfer of data from staging to production,
    # purge all data from staging tables
    with TaskGroup("Delete_openaq_staging_data") as delete_openaq_staging_data:
        # Transfer raw tables to production tables
        delete_openaq_staging_data_operator = PostgresOperator(
            task_id="delete_openaq_staging_data",
            postgres_conn_id=POSTGRES_STAGING_CONN_ID,
            sql='select 1 ; --do nothing' #SqlQueries.delete_openaq_staging_data
        )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


# ............................................
# Defining the main DAG
# ............................................

start_operator >> create_tables
create_tables >> run_file_import
run_file_import >> delete_openaq_staging_data
delete_openaq_staging_data >> end_operator
