from datetime import date, datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
#from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

# Variable needs to be defined here so that
# it can be found by the Airflow engine when initializig the DAG
Variable.delete('most_recent_noaa_data')
Variable.set('most_recent_noaa_data', noaa.data_available_from.strftime('%Y%m%d'))

default_start_date = datetime(year=2021,month=1,day=31)

postgres_conn_id = 'postgres'
postgres_create_fact_tables_file = './dags/sql/create_facts_tables.sql'
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

def get_date_of_most_recent_noaa_facts() -> None :
    """ Get the date of the most recent fact in
        the public.weather_data_raw data from postgres
        Set *most_recent_noaa_data* variable in airflow
    """

    sql_cmd = f"""SELECT max(date_) as max_date FROM public.weather_data_raw"""
    postgres = PostgresHook(postgres_conn_id)
    connection = postgres.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_cmd)
    most_recent = cursor.fetchone()
    try:
        most_recent_day = datetime.strptime(most_recent[0],'%Y%m%d')
    except:
        most_recent_day = noaa.data_available_from
    else:
        # Add one day to avoid complications with
        # Dec 31st dates
        most_recent_day += timedelta(days=1)
    print(f'Most recent NOAA data is as of: {most_recent_day}')
    Variable.delete('most_recent_noaa_data')
    Variable.set('most_recent_noaa_data', most_recent_day.strftime('%Y%m%d'))


with DAG('noaa_facts_dag',
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

    # Create NOAA fact tables in Staging Database (Postgresql)
    #
    create_noaa_fact_tables_operator = CreateTablesOperator(
        task_id='Create_noaa_fact_tables',
        postgres_conn_id=postgres_conn_id,
        sql_query_file=postgres_create_fact_tables_file
        )

    # Get date of most recent NOAA data in the Postgres staging tables
    #
    get_date_of_most_recent_noaa_facts_operator = PythonOperator(
        task_id='Get_date_of_most_recent_noaa_facts',
        python_callable=get_date_of_most_recent_noaa_facts
        )
    
    # Select all facts for date = {{ DS }} from the NOAA S3 bucket
    # and store them as a csv file in the local Staging Area (Filesystem)
    #
    select_noaa_data_from_s3_operator = SelectFromNOAAS3ToStagingOperator(
        task_id='Select_noaa_data_from_s3',
        aws_credentials=noaa.source_params['aws_credentials'],
        s3_bucket=noaa.source_params['s3_bucket'],
        s3_prefix='csv.gz',
        s3_table_file='{{ execution_date.year }}.csv.gz',
        most_recent_data_date='{{ var.value.most_recent_noaa_data }}',
        execution_date='{{ ds_nodash }}',
        real_date=date.today().strftime('%Y%m%d'),
        local_path=os.path.join(noaa.staging_location,'facts')
        )

    # Load the NOAA fact data for {{ DS }} from csv file on local Staging
    # into the tables prepared on Staging Database (Postgresql)
    #
    load_noaa_fact_tables_into_postgres_operator = LocalStageToPostgresOperator(
        task_id='Load_noaa_fact_tables_into_postgres',
        postgres_conn_id=postgres_conn_id,
        table='public.weather_data_raw',
        delimiter=',',
        truncate_table=True,
        local_path=os.path.join(noaa.staging_location,'facts'),
        )


    # Run quality checks on fact data
    #
    check_fact_quality_operator = DummyOperator(task_id='Check_fact_quality')

    end_operator = DummyOperator(task_id='Stop_execution')


# ............................................
# Defining the DAG structure
# ............................................

start_operator >> create_noaa_fact_tables_operator

create_noaa_fact_tables_operator >> get_date_of_most_recent_noaa_facts_operator
get_date_of_most_recent_noaa_facts_operator >> select_noaa_data_from_s3_operator
select_noaa_data_from_s3_operator >> load_noaa_fact_tables_into_postgres_operator
load_noaa_fact_tables_into_postgres_operator >> check_fact_quality_operator
check_fact_quality_operator >> end_operator


