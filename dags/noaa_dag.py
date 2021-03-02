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
from operators.local_csv_to_postgres import LocalCSVToPostgresOperator
from operators.get_most_recent_data_date import GetMostRecentDataDateOperator
from operators.noaa.download_noaa_dim_file_to_staging import DownloadNOAADimFileToStagingOperator
from operators.noaa.select_from_noaa_s3_to_staging import SelectFromNOAAS3ToStagingOperator

from helpers.sql_queries import SqlQueries
from helpers.data_quality_checks import DataQualityChecks


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
#  connectivity.
noaa_config: dict = Variable.get("noaa_config", deserialize_json=True)
NOAA_AWS_CREDS: str = noaa_config['source_params']['aws_credentials']
NOAA_S3_BUCKET: str = noaa_config['source_params']['s3_bucket']
NOAA_S3_KEYS: list = noaa_config['source_params']['s3_keys']
NOAA_S3_FACT_DELIM: str  = noaa_config['source_params']['s3_fact_delimiter']
NOAA_S3_FACT_PREFIX: str = noaa_config['source_params']['s3_fact_prefix']
NOAA_S3_FACT_COMPRESSION: str = noaa_config['source_params']['s3_fact_compression']
NOAA_S3_FACT_FORMAT: str = noaa_config['source_params']['s3_fact_format']
NOAA_DATA_AVAILABLE_FROM: str = noaa_config['data_available_from']
NOAA_STAGING_LOCATION: str = os.path.join(AIRFLOW_HOME, noaa_config['staging_location'])

## Definitions in ./variables/general.json
general_config: dict = Variable.get("general", deserialize_json=True)
CSV_QUOTE_CHAR = general_config['csv_quote_char']
CSV_DELIMITER = general_config['csv_delimiter']
NOAA_STAGING_SCHEMA = general_config['noaa_staging_schema']
PRODUCTION_SCHEMA = general_config['production_schema']

dags_config: dict = Variable.get("dags", deserialize_json=True)
NOAA_DAG_NAME   = dags_config['noaa_dag_name']
PROCESS_DAG_NAME =  dags_config['process_dag_name']

NOAA_STAGING_DIM_RAW = os.path.join(NOAA_STAGING_LOCATION,'dimensions_raw')
NOAA_STAGING_DIM_CSV = os.path.join(NOAA_STAGING_LOCATION,'dimensions_csv')
NOAA_STAGING_FACTS = os.path.join(NOAA_STAGING_LOCATION,'facts')

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

# def get_date_of_most_recent_noaa_facts() -> None:
#     """
#     Get the date of the most recent fact in the noaa_staging_schema.weather_data_raw data
#     from postgres and set *noaa_most_recent_data* variable in airflow
#     """
#
#     # Get date of most recent data from production table
#     postgres = PostgresHook(POSTGRES_STAGING_CONN_ID)
#     connection = postgres.get_conn()
#     cursor = connection.cursor()
#     cursor.execute(SqlQueries.noaa_most_recent_data)
#     most_recent = cursor.fetchone()
#     try:
#         most_recent_day = datetime.strptime(most_recent[0],'%Y%m%d')
#     except:
#         most_recent_day = NOAA_DATA_AVAILABLE_FROM
#     else:
#         # Add one day to avoid complications with
#         # Dec 31st dates
#         most_recent_day += timedelta(days=1)
#     print(f'Most recent NOAA data is as of: {most_recent_day}')
#     Variable.delete('noaa_most_recent_data')
#     Variable.set('noaa_most_recent_data', most_recent_day) #.strftime('%Y%m%d'))

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
                    schema=f'{PRODUCTION_SCHEMA}',
                    table='f_climate_data',
                    year=year
                    )
        cursor.execute(f_sql)
    connection.commit()


with DAG(NOAA_DAG_NAME,
          default_args = DEFAULT_ARGS,
          description = 'Load NOAA data from NOAA S3 to local Postgres',
          catchup = False,
          start_date = DEFAULT_START_DATE,
          concurrency = 8,
          max_active_runs = 8,
          schedule_interval = '0 23 * * *' # run daily at midnight
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    # -----------------------------------------------------
    #   Create tables in noaa_staging and production schema
    # -----------------------------------------------------
    with TaskGroup("Create_postgres_tables") as create_tables:
        # Create NOAA dimension tables in Staging Database (Postgresql)
        create_noaa_tables_operator = CreateTablesOperator(
            task_id = 'Create_noaa_tables',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            sql_query_file = os.path.join(AIRFLOW_HOME,
                                          POSTGRES_CREATE_NOAA_TABLES_FILE),
            )
        # Populate d_kpi_names table with most relevant KPI names
        populate_kpi_names_table_operator = PostgresOperator(
            task_id="populate_kpi_names_table",
            postgres_conn_id=POSTGRES_STAGING_CONN_ID,
            sql=SqlQueries.noaa_populate_d_kpi_table
        )
        # Create partition tables for f_climate_data
        create_partition_tables_operator = PythonOperator(
            task_id = 'Create_partition_tables',
            python_callable = create_partition_tables
            )
        create_noaa_tables_operator >> populate_kpi_names_table_operator
        create_noaa_tables_operator >> create_partition_tables_operator

    # -----------------------------------------------------
    # Run import of Dimension and Fact data in parallel
    # -----------------------------------------------------
    with TaskGroup("Run_dimension_import") as run_dim_import:
        # Load relevant dimension and documentation files from the
        # NOAA S3 bucket into the local Staging Area (Filesystem)
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

        # Reformat the fixed-width files into a format that Postgresql can deal with
        with TaskGroup("reformat_noaa_dims") as reformat_noaa_dims:
            s3_index = 2
            reformat_noaa_countries_operator = ReformatFixedWidthFileOperator(
                task_id = f'Reformat_{NOAA_S3_KEYS[s3_index][:-4]}_dim_file',
                filename = NOAA_S3_KEYS[s3_index],
                local_path_fixed_width = NOAA_STAGING_DIM_RAW,
                local_path_csv = NOAA_STAGING_DIM_CSV,
                column_names = ['country_id', 'country'],
                column_positions = [0, 3],
                delimiter = f'{CSV_DELIMITER}',
                quote = f'{CSV_QUOTE_CHAR}',
                add_header = True,
                remove_original_file = False,
                )

            s3_index = 3
            reformat_noaa_inventory_operator = ReformatFixedWidthFileOperator(
                task_id = f'Reformat_{NOAA_S3_KEYS[s3_index][:-4]}_dim_file',
                filename = NOAA_S3_KEYS[s3_index],
                local_path_fixed_width = NOAA_STAGING_DIM_RAW,
                local_path_csv = NOAA_STAGING_DIM_CSV,
                column_names = ['id', 'latitude','longitude','element','from_year','until_year'],
                column_positions = [0, 11, 20, 30, 35, 40],
                delimiter = f'{CSV_DELIMITER}',
                quote = f'{CSV_QUOTE_CHAR}',
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
                delimiter = f'{CSV_DELIMITER}',
                quote = f'{CSV_QUOTE_CHAR}',
                add_header = True,
                remove_original_file = False,
                )

        # In case the data changed, load the NOAA dimension data from csv file on
        # local Staging into the tables prepared on Staging Database (Postgresql)
        #
        # Currently, there is no mechanism to stop the import in case the staging
        # files have not changed. Dimension data is hence reloaded every time the
        # DAG is executed.
        #
        with TaskGroup("import_noaa_dims") as import_noaa_dims:
            s3_index = 2
            import_noaa_country_operator = LocalCSVToPostgresOperator(
                task_id = f'Import_{NOAA_S3_KEYS[s3_index][:-4]}_into_postgres',
                postgres_conn_id = POSTGRES_STAGING_CONN_ID,
                table = f'{NOAA_STAGING_SCHEMA}.ghcnd_countries_raw',
                delimiter = f'{CSV_DELIMITER}',
                quote = f'{CSV_QUOTE_CHAR}',
                truncate_table = True,
                local_path = NOAA_STAGING_DIM_CSV,
                file_pattern = f'{NOAA_S3_KEYS[s3_index]}',
                gzipped = False
                )
            s3_index = 3
            import_noaa_inventory_operator = LocalCSVToPostgresOperator(
                task_id = f'Import_{NOAA_S3_KEYS[s3_index][:-4]}_into_postgres',
                postgres_conn_id = POSTGRES_STAGING_CONN_ID,
                table = f'{NOAA_STAGING_SCHEMA}.ghcnd_inventory_raw',
                delimiter = f'{CSV_DELIMITER}',
                quote = f'{CSV_QUOTE_CHAR}',
                truncate_table = True,
                local_path = NOAA_STAGING_DIM_CSV,
                file_pattern = f'{NOAA_S3_KEYS[s3_index]}',
                gzipped = False
                )
            s3_index = 5
            import_noaa_stations_operator = LocalCSVToPostgresOperator(
                task_id = f'Import_{NOAA_S3_KEYS[s3_index][:-4]}_into_postgres',
                postgres_conn_id = POSTGRES_STAGING_CONN_ID,
                table = f'{NOAA_STAGING_SCHEMA}.ghcnd_stations_raw',
                delimiter = f'{CSV_DELIMITER}',
                quote = f'{CSV_QUOTE_CHAR}',
                truncate_table = True,
                local_path = NOAA_STAGING_DIM_CSV,
                file_pattern = f'{NOAA_S3_KEYS[s3_index]}',
                gzipped = False
                )

        # Run quality checks on dimension data
        check_dim_quality_operator = DataQualityOperator(
            task_id='Check_dim_quality',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            dq_checks = DataQualityChecks.dq_checks_noaa_dim,
            )

        with TaskGroup("Load_dims_into_production") as load_dims_into_production:
            # Transfer raw tables to production tables
            load_noaa_countries_operator = PostgresOperator(
                task_id="load_noaa_countries",
                postgres_conn_id=POSTGRES_STAGING_CONN_ID,
                sql=SqlQueries.load_noaa_countries
            )

            # Transfer raw tables to production tables
            load_noaa_stations_operator = PostgresOperator(
                task_id="load_noaa_stations",
                postgres_conn_id=POSTGRES_STAGING_CONN_ID,
                sql=SqlQueries.load_noaa_stations
            )

            # Transfer raw tables to production tables
            load_noaa_inventory_operator = PostgresOperator(
                task_id="load_noaa_inventory",
                postgres_conn_id=POSTGRES_STAGING_CONN_ID,
                sql=SqlQueries.load_noaa_inventory
            )
        # ............................................
        # Defining TaskGroup DAG
        # ............................................
        download_noaa_dims >> reformat_noaa_dims
        reformat_noaa_dims >> import_noaa_dims
        import_noaa_dims >> check_dim_quality_operator
        check_dim_quality_operator >> load_dims_into_production
        #[download_noaa_dims] >> dummy1_operator >> [reformat_noaa_dims]
        #[reformat_noaa_dims] >> dummy2_operator >> [import_noaa_dims]
        #[import_noaa_dims] >> check_dim_quality_operator
        #check_dim_quality_operator >> [load_dims_into_production]

    # -----------------------------------------------------
    # Run import of Dimension and Fact data in parallel
    # -----------------------------------------------------
    with TaskGroup("Run_fact_import") as run_facts_import:

        # Get date of most recent NOAA data in the Postgres production tables
        get_most_recent_noaa_data_date_operator = GetMostRecentDataDateOperator(
            task_id = 'Get_date_of_most_recent_noaa_fact',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            schema = PRODUCTION_SCHEMA,
            table = 'f_climate_data',
            where_clause = "source = 'noaa'",
            date_field = 'date_',
            airflow_var_name = 'noaa_most_recent_data'
            )
        # get_date_of_most_recent_noaa_facts_operator = PythonOperator(
        #     task_id = 'Get_date_of_most_recent_noaa_facts',
        #     python_callable = get_date_of_most_recent_noaa_facts
        #     )

        # Select all facts for date == {{ DS }} from the NOAA S3 bucket
        # and store them as a csv file in the local Staging Area (Filesystem)
        select_noaa_data_from_s3_operator = SelectFromNOAAS3ToStagingOperator(
            task_id = 'Select_noaa_data_from_s3',
            aws_credentials = NOAA_AWS_CREDS,
            s3_bucket = NOAA_S3_BUCKET,
            s3_prefix = NOAA_S3_FACT_PREFIX,
            s3_table_file = '{{ execution_date.year }}.csv.gz',
            most_recent_data_date = '{{ var.value.noaa_most_recent_data }}',
            execution_date = '{{ ds }}',
            real_date = date.today().strftime('%Y-%m-%d'),
            local_path = NOAA_STAGING_FACTS,
            fact_delimiter = NOAA_S3_FACT_DELIM,
            quotation_char = CSV_QUOTE_CHAR
            )

        # Load the NOAA fact data for {{ DS }} from csv file on local Staging
        # into the tables prepared on Staging Database (Postgresql)
        load_noaa_fact_tables_into_postgres_operator = LocalCSVToPostgresOperator(
            task_id = 'Load_noaa_fact_tables_into_postgres',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            table = f'{NOAA_STAGING_SCHEMA}.f_weather_data_raw',
            delimiter = NOAA_S3_FACT_DELIM,
            truncate_table = True,
            local_path = NOAA_STAGING_FACTS,
            file_pattern = "*.csv.gz",
            gzipped = True
            )

        # Run quality checks on raw fact data
        check_fact_quality_operator = DataQualityOperator(
            task_id='Check_fact_quality',
            postgres_conn_id = POSTGRES_STAGING_CONN_ID,
            dq_checks = DataQualityChecks.dq_checks_noaa_facts,
            )

        # Finally, insert quality-checked data from Postgres Staging
        # into the "production" database
        transform_noaa_facts_operator = PostgresOperator(
            task_id="transform_noaa_facts",
            postgres_conn_id=POSTGRES_STAGING_CONN_ID,
            sql=SqlQueries.transform_noaa_facts
        )
        # ............................................
        # Defining TaskGroup DAG
        # ............................................
        get_most_recent_noaa_data_date_operator >> select_noaa_data_from_s3_operator
        select_noaa_data_from_s3_operator >> load_noaa_fact_tables_into_postgres_operator
        load_noaa_fact_tables_into_postgres_operator >> check_fact_quality_operator
        check_fact_quality_operator >> transform_noaa_facts_operator


    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


# ............................................
# Defining the main DAG
# ............................................

start_operator >> create_tables
create_tables >> run_dim_import
create_tables >> run_facts_import
run_dim_import >> end_operator
run_facts_import >> end_operator
