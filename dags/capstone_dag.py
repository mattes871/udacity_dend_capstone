from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.create_tables import CreateTablesOperator
from helpers.sql_queries import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
#
#start_date = datetime.utcnow()
#  create_redshift_tables_file = '/home/workspace/airflow/create_tables.sql'
#  json_paths_bucket = 's3://udacity-8fnmyev4xc5y-jsonpaths/jsonpaths.json'

postgres_create_tables_file = '/home/workspace/airflow/plugins/helpers/create_tables.sql'

default_args = {
    'owner': 'matkir',
    'depends_on_past': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2019,1,12),
    'region': 'eu-central-1'
}

# The following DAG performs the functions:
#
#       1. Load Log- and Song-data from S3 into staging tables on Amazon RedShift
#       2. Extract facts data from 'staging_events' table into 'songplays' table
#       3. Extract dimension data from 'songplays', 'staging_songs' and 'staging_events'
#          into dimension tables 'users', 'songs', 'artists', and 'time'
#       4. Run data quality checks on facts and dimension tables
dag = DAG('s3_to_postgres_dag',
          default_args = default_args,
          description = 'Load and transform data in Postgresql with Airflow',
          catchup = False,
          max_active_runs = 1, # to prevent Airflow from running multiple days/hours at the same time
          schedule_interval = '@monthly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  
                               dag=dag)

#
# Create all necessary tables on Redshift
#
create_tables_on_postgres = CreateTablesOperator(
    task_id = 'Create_tables_on_postgres',
    dag = dag,
    postgres_conn_id = 'airflow',
    sql_query_file = postgres_create_tables_file
)

end_operator = DummyOperator(task_id='Stop_execution',
                             dag=dag)

#  #
#  # Load eventsdata from S3 to Postgresql machine.
#  #
#  stage_events_to_redshift = StageToRedshiftOperator(
#      task_id = '',
#      dag = dag,
#      #redshift_conn_id = 'redshift',
#      aws_credentials_id = 'aws_credentials',
#      table = 'staging_data',
#      s3_bucket = 'noaa-ghcn-pds',
#      s3_key = f'csv.gz/2021.csv.gz',
#      ## example of 's3_key' with execution_date info:
#      ## s3_key = 'log_data/{{ execution_date.year }}/{{ execution_date.month }}/{{ ds }}',
#      region = AWS_REGION,
#      json = json_paths_bucket,       # required to match upper case field names in json file
#      truncate_table = True           # prevent staging table from growing with every run
#  )
#





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


start_operator >> create_tables_on_postgres >> end_operator

#  start_operator >> create_tables_on_redshift
#  create_tables_on_redshift >> [stage_events_to_redshift, stage_songs_to_redshift]
#  stage_events_to_redshift >> load_songplays_table
#  stage_songs_to_redshift >> load_songplays_table
#  load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
#                           load_artist_dimension_table, load_time_dimension_table]
#  [load_user_dimension_table, load_song_dimension_table,
#   load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
#  run_quality_checks >> end_operator
