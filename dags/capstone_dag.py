from datetime import date, datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
#from airflow.operators.python import PythonOperator
#from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from operators.create_tables import CreateTablesOperator
from operators.stage_postgres import StageToPostgresOperator
from operators.select_from_s3_to_staging import SelectFromS3ToStagingOperator
from operators.copy_s3_files_to_staging import CopyS3FilesToStagingOperator

from helpers.sql_queries import SqlQueries
from helpers.source_data_class import SourceDataClass

AWS_KEY    = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
AWS_REGION = os.environ.get('AWS_REGION', default='eu-central-1')


noaa = SourceDataClass(
    source_name='noaa',
    description="""Climate KPIs from 200k stations worldwide, dating back as far
    as 1763""",
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
    target_dir='./staging_files/noaa',
    version='v2021-02-05')


postgres_create_tables_file = './plugins/helpers/create_tables.sql'
csv_delimiter = ','

default_args = {
    'owner': 'matkir',
    'depends_on_past': False,
    'retries': 3,
    'catchup': True, #False,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(year=2021,month=1,day=31),
    'region': 'eu-central-1'
}



#
#  def s3_test_func():
#      """This is a test"""
#      s3_hook = S3Hook('aws_credentials')
#      result = s3_hook.list_keys(
#                  bucket_name=BUCKET_NAME,
#                  delimiter='/'
#                  )
#      print(f'------------------------- load_keys(): {result}')
#      result = s3_hook.list_prefixes(
#                  bucket_name=BUCKET_NAME,
#                  delimiter='/'
#                  )
#      print(f'------------------------- load_prefixes(): {result}')
#      result = s3_hook.select_key(
#                key='csv.gz/1971.csv.gz',
#                bucket_name=BUCKET_NAME,
#                expression="select s._1 as a, s._2 as b, s._3 as c from s3object s where s._2='19710111' limit 5",
#                input_serialization={
#                    'CSV': {
#                           'FileHeaderInfo': 'NONE',
#                           'FieldDelimiter': ','
#                        },
#                    'CompressionType': 'GZIP'
#                    }
#                )
#      print(f'------------------------- select_key(): {result}')
#
#      credentials = s3_hook.get_credentials()
#      copy_options = ''
#
#      copy_statement = f"""
#          COPY public.weather_data_raw
#          FROM 's3://{BUCKET_NAME}/csv/1971.csv'
#          with credentials
#          'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key}'
#          {copy_options};
#      """
#      print(f'COPY STATEMENT: \n{copy_statement}')

# The following DAG performs the functions:
#



with DAG('climate_datamart_dag',
          default_args = default_args,
          description = 'Load climate data and create a regular report',
          catchup = True, #False,
          start_date = datetime(year=2021,month=1,day=31)
          concurrency = 4,
          max_active_runs = 4, # to prevent Airflow from running 
                               # multiple days/hours at the same time
          schedule_interval = '@daily'
        ) as dag:

    execution_date_str = "{{ ds_nodash }}"

    start_operator = DummyOperator(task_id='Begin_execution')

    # Create NOAA tables in Staging Database (Postgresql)
    #
    create_noaa_tables_operator = DummyOperator(task_id='Create_noaa_tables')

    # >>>>> Make sure that dim- and doc-files are loaded only once
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    #
    # Load relevant dimension and documentation files from the
    # NOAA S3 bucket into the local Staging Area (Filesystem)
    #
    copy_noaa_s3_files_to_staging_operator = CopyS3FilesToStagingOperator(
        task_id='Copy_noaa_s3_files_to_staging',
        aws_credentials=noaa.source_params['aws_credentials'],
        s3_bucket=noaa.source_params['s3_bucket'],
        s3_prefix='', 
        s3_files=noaa.source_params['files'],
        staging_location=noaa.target_dir
        )

    # In case it changed, load the NOAA dimension from csv file on 
    # local Staging into the tables prepared on Staging Database (Postgresql)
    #
    load_noaa_dim_tables_into_postgres_operator = \
            DummyOperator(task_id='Load_noaa_dim_tables_into_postgres')

    # Run quality checks on dimension data
    #
    check_dim_quality_operator = DummyOperator(task_id='Check_dim_quality')


    # Select all facts for date = {{ DS }} from the NOAA S3 bucket
    # and store them as a csv file in the local Staging Area (Filesystem)
    #
    select_noaa_data_from_s3_operator = SelectFromS3ToStagingOperator(
        task_id='Select_noaa_data_from_s3',
        aws_credentials=noaa.source_params['aws_credentials'],
        s3_bucket=noaa.source_params['s3_bucket'],
        s3_prefix='csv.gz',
        s3_table_file='{{ execution_date.year }}.csv.gz',
        execution_date_str=execution_date_str,
        real_date_str=date.today().strftime("%Y-%m-%d"),
        staging_location=noaa.target_dir
        )

    # Load the NOAA fact data for {{ DS }} from csv file on local Staging
    # into the tables prepared on Staging Database (Postgresql)
    #
    load_noaa_fact_tables_into_postgres_operator = \
            DummyOperator(task_id='Load_noaa_fact_tables_into_postgres')

    # Run quality checks on fact data
    #
    check_fact_quality_operator = DummyOperator(task_id='Check_fact_quality')

    end_operator = DummyOperator(task_id='Stop_execution')


# ............................................
# Defining the DAG
# ............................................

start_operator >> create_noaa_tables_operator

create_noaa_tables_operator >> copy_noaa_s3_files_to_staging_operator
copy_noaa_s3_files_to_staging_operator >> load_noaa_dim_tables_into_postgres_operator
load_noaa_dim_tables_into_postgres_operator >> check_dim_quality_operator
check_dim_quality_operator >> end_operator

create_noaa_tables_operator >> select_noaa_data_from_s3_operator
select_noaa_data_from_s3_operator >> load_noaa_fact_tables_into_postgres_operator
load_noaa_fact_tables_into_postgres_operator >> check_fact_quality_operator
check_fact_quality_operator >> end_operator





#  #
#  # Load song data data from S3 to RedShift. Use the s3_key
#  # "song_data" and the s3_bucket "udacity-dend"
#  #
#  stage_songs_to_redshift = StageToRedshiftOperator(
#      task_id = 'Stage_songs',
#      dag = dag,
#      redshift_conn_id = 'redshift',
#      aws_credentials_id = 'aws_credentials',
#      table = 'staging_songs',
#      s3_bucket = 'udacity-dend',
#      s3_key = 'song_data',
#      region = 'us-west-2',
#      json = 'auto',
#      truncate_table = True           # prevent staging_songs from growing with every run
#  )
#
#  #
#  # Extract all songplay data from
#  # events and song data already stored in Redshift
#  #
#  load_songplays_table = LoadFactOperator(
#      task_id='Load_songplays_fact_table',
#      dag=dag,
#      redshift_conn_id = 'redshift',
#      table = 'songplays',
#      sql_query = SqlQueries.songplay_table_insert,
#      truncate_table = True
#  )
#
#  load_user_dimension_table = LoadDimensionOperator(
#      task_id='Load_user_dim_table',
#      dag=dag,
#      redshift_conn_id = 'redshift',
#      table = 'users',
#      sql_query = SqlQueries.user_table_insert,
#      truncate_table = True
#  )
#
#  load_song_dimension_table = LoadDimensionOperator(
#      task_id='Load_song_dim_table',
#      dag=dag,
#      redshift_conn_id = 'redshift',
#      table = 'songs',
#      sql_query = SqlQueries.song_table_insert,
#      truncate_table = True
#  )
#
#  load_artist_dimension_table = LoadDimensionOperator(
#      task_id='Load_artist_dim_table',
#      dag=dag,
#      redshift_conn_id = 'redshift',
#      table = 'artists',
#      sql_query = SqlQueries.artist_table_insert,
#      truncate_table = True
#  )
#
#  load_time_dimension_table = LoadDimensionOperator(
#      task_id='Load_time_dim_table',
#      dag=dag,
#      redshift_conn_id = 'redshift',
#      table = 'time',
#      sql_query = SqlQueries.time_table_insert,
#      truncate_table = True
#  )
#
#  dq_checks=[
#      {'sql': 'SELECT COUNT(*) FROM users WHERE userid is null', 'expected': True, 'value': 0},
#      {'sql': 'SELECT COUNT(*) FROM songs WHERE songid is null', 'expected': True,  'value': 0},
#      {'sql': 'SELECT COUNT(*) FROM artists WHERE artistid is null', 'expected': True,  'value': 0},
#      {'sql': 'SELECT COUNT(*) FROM songplays WHERE playid is null', 'expected': True,  'value': 0},
#      {'sql': 'SELECT COUNT(*) FROM users', 'expected': False, 'value': 0},
#      {'sql': 'SELECT COUNT(*) FROM songs', 'expected': False,  'value': 0},
#      {'sql': 'SELECT COUNT(*) FROM artists', 'expected': False,  'value': 0},
#      {'sql': 'SELECT COUNT(*) FROM songplays', 'expected': False,  'value': 0}
#      ]
#
#  run_quality_checks = DataQualityOperator(
#      task_id='Run_data_quality_checks',
#      redshift_conn_id = 'redshift',
#      dq_checks = dq_checks,
#      dag=dag
#  )
#



#  start_operator >> create_tables_on_redshift
#  create_tables_on_redshift >> [stage_events_to_redshift, stage_songs_to_redshift]
#  stage_events_to_redshift >> load_songplays_table
#  stage_songs_to_redshift >> load_songplays_table
#  load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
#                           load_artist_dimension_table, load_time_dimension_table]
#  [load_user_dimension_table, load_song_dimension_table,
#   load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
#  run_quality_checks >> end_operator
