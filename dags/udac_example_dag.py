# remove __pycache__
# find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    aws_conn_id='aws_credentials'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    aws_conn_id='aws_credentials'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    fact_table_query=SqlQueries.songplay_table_insert,
    destination_table="songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table_query=SqlQueries.user_table_insert,
    destination_table="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table_query=SqlQueries.song_table_insert,
    destination_table="songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table_query=SqlQueries.artist_table_insert,
    destination_table="artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table_query=SqlQueries.time_table_insert,
    destination_table="time"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Airflow dependencies
# tasks that fan out
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# tasks that funnel in
load_songplays_table << stage_events_to_redshift
load_songplays_table << stage_songs_to_redshift

# fan out: 1 to 4
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# funnel in: 4 to 1
run_quality_checks << load_song_dimension_table
run_quality_checks << load_user_dimension_table
run_quality_checks << load_artist_dimension_table
run_quality_checks << load_time_dimension_table

run_quality_checks >> end_operator
