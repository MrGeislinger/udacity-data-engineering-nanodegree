from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past' : False,
    'start_date': datetime(2020, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Creates table on Redshift
create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

# Stage events from JSON
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    copy_json_option='s3://udacity-dend/log_json_path.json',
    region='us-west-2',
    conn_id='redshift',
    dag=dag
)

# Stage songs to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    copy_json_option='auto',
    region='us-west-2',
    conn_id='redshift',
    dag=dag
)

# Insert data into songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    conn_id='redshift',
    table='songplays',
    sql=SqlQueries.songplay_table_insert,
    dag=dag
)

## Insert data into dimensional tables

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    conn_id='redshift',
    table='songs',
    sql=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    conn_id='redshift',
    table='artists',
    sql=SqlQueries.artist_table_insert,
    append=True,
    primary_key="artistid",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    conn_id='redshift',
    table='time',
    sql=SqlQueries.time_table_insert,
    dag=dag
)

# Check the data quality with a test query
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    conn_id='redshift',
    test_query='SELECT COUNT(*) FROM songs WHERE songid IS NULL;',
    test_result=0,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

## Order of operations specified in project
# Start with creating tables for Redshift
start_operator >> create_tables
# Read to Redshift
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
# Loading the fact table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
# loading the dimension tables
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
# Check data quality
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
# Complete execution
run_quality_checks >> end_operator