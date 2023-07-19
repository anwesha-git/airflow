from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.stage_redshift import StageToRedshiftOperator
from airflow.operators.load_fact import LoadFactOperator
from airflow.operators.load_dimension import LoadDimensionOperator
from airflow.operators.data_quality import DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'Sparkify',
    'start_date': pendulum.now(),
    'retries': 3 ,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False ,
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def sparkify_data_pipeline():

    start_operator = DummyOperator(task_id='Begin_execution')

    #loading data from S3 to staging_events table
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        s3_bucket="udacity-dend",
        s3_key="s3://udacity-dend/log_data",
        aws_credentials_id="aws_credentials",
        region='us-west-2',
        json_path="s3://udacity-dend/log_json_path.json"
    )

    #loading data from S3 to staging_songs table
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        s3_bucket="udacity-dend",
        s3_key="s3://udacity-dend/song_data",
        aws_credentials_id="aws_credentials",
        region='us-west-2',
        json_path="auto"
    )

    #loading data from staging tables to songplays table
    load_songplays_fact_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",    
        table="songplays",
        sql_query=SqlQueries.songplay_table_insert,
        truncate=True
    )

    #loading data from staging tables to user table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table="user",
        sql_query=SqlQueries.user_table_insert,
        truncate=True
    )

    #loading data from staging tables to song table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table="song",
        sql_query=SqlQueries.song_table_insert,
        truncate=True
    )

    #loading data from staging tables to artist table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table="artist",
        sql_query=SqlQueries.artist_table_insert,
        truncate=True
    )

    #loading data from staging tables to time table
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table="time",
        sql_query=SqlQueries.time_table_insert,
        truncate=True
    )

    #running quality checks on tables
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=[ "songplays", "song", "artist", "time", "user"]
    )

    end_operator = DummyOperator(task_id='End_execution')

#setting up dependencies between tasks
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
 
stage_events_to_redshift >> load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_fact_table

load_songplays_fact_table >> load_song_dimension_table >> run_quality_checks
load_songplays_fact_table >> load_user_dimension_table >> run_quality_checks
load_songplays_fact_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_fact_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

#Defining the DAG
sparkify_data_pipeline_dag = sparkify_data_pipeline()