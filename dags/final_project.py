from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'Dave',
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
)
def final_project():

    fixed_date = datetime(2018, 11, 1)
    
    start_operator = DummyOperator(task_id='Begin_execution')

    create_tables = PostgresOperator(
        task_id='Create_tables',
        postgres_conn_id='redshift',
        sql='create_tables.sql',
        split_statements=True
    )
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        table='staging_events',
        s3_bucket='aws-stud003-final',
        s3_key=f'log-data/{fixed_date.year}/{fixed_date.month}/{fixed_date.strftime("%Y-%m-%d")}-events.json',
        json_path='s3://aws-stud003-final/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        table='staging_songs',
        s3_bucket='aws-stud003-final',
        s3_key='song-data/A/',
        json_path='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        query = SqlQueries.songplay_table_insert,
        destination_table = 'songplays'
    )

    # load_user_dimension_table = DummyOperator(task_id='Load_user_dim_table')
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = 'redshift',
        destination_table = 'users',
        query = SqlQueries.user_table_insert,
        mode = 'truncate-insert'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = 'redshift',
        destination_table = 'songs',
        query = SqlQueries.song_table_insert,
        mode = 'truncate-insert',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = 'redshift',
        destination_table = 'artists',
        query = SqlQueries.artist_table_insert,
        mode = 'truncate-insert'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = 'redshift',
        destination_table = 'time',
        query = SqlQueries.time_table_insert,
        mode = 'truncate-insert'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = 'redshift',
        test_cases = [
            {
                'sql': "SELECT COUNT(*) FROM songs;",
                'expected': 14896
            },
            {
                'sql': "SELECT COUNT(*) FROM users;",
                'expected': 4
            },
            {
                'sql': "SELECT COUNT(*) FROM artists;",
                'expected': 10025
            },
            {
                'sql': 'SELECT COUNT(*) FROM "time";',
                'expected': 11
            }
        ]
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> create_tables
    create_tables >> stage_events_to_redshift
    create_tables >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
