from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from custom_operators.stage_redshift import StageToRedshiftOperator
from custom_operators.load_fact import LoadFactOperator
from custom_operators.load_dimension import LoadDimensionOperator
from custom_operators.data_quality import DataQualityOperator

# I modified this one, Airflow was not reading songplay_table_insert
from udacity.common.final_project_sql_statements import SqlQueries

#Start of DAG
default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False, #DAG does not have dependencies on past runs
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_entry': False,
    'catchup': False, #Turn off catchup
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *', # Runs once an hour at the beginning of the hr
    max_active_runs=1
)
def final_project1():

        
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='peter-automation-s3',
        s3_key='log_data',
        file_format='s3://udacity-dend/log_json_path.json',
        region='us-east-1'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='peter-automation-s3',
        s3_key='song-data/A/A/',
        file_format='auto',
        region='us-east-1'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert,
        truncate=True #This allows switch between append and truncate in load dimention
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql=SqlQueries.song_table_insert,
        truncate=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql=SqlQueries.artist_table_insert,
        truncate=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert,
        truncate=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tests=[
            {'check_sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM songs WHERE song_id IS NULL', 'expected_result': 0}
        ]
    )

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

final_project_dag1 = final_project1()
