import pendulum
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

@dag(
    start_date=pendulum.now()
)
def create_tables_dag():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_songplays_table = PostgresOperator(
        task_id='Create_songplays_table',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE IF NOT EXISTS songplays (
                songplay_id VARCHAR PRIMARY KEY,
                start_time TIMESTAMP NOT NULL,
                userid INT NOT NULL,
                level VARCHAR,
                song_id VARCHAR,
                artist_id VARCHAR,
                sessionid INT,
                location VARCHAR,
                useragent VARCHAR
            );
        """
    )

    create_users_table = PostgresOperator(
        task_id='Create_users_table',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                userid INT PRIMARY KEY,
                firstname VARCHAR,
                lastname VARCHAR,
                gender VARCHAR,
                level VARCHAR
            );
        """
    )

    create_songs_table = PostgresOperator(
        task_id='Create_songs_table',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE IF NOT EXISTS songs (
                song_id VARCHAR PRIMARY KEY,
                title VARCHAR,
                artist_id VARCHAR,
                year INT,
                duration FLOAT
            );
        """
    )

    create_artists_table = PostgresOperator(
        task_id='Create_artists_table',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE IF NOT EXISTS artists (
                artist_id VARCHAR PRIMARY KEY,
                artist_name VARCHAR,
                artist_location VARCHAR,
                artist_latitude FLOAT,
                artist_longitude FLOAT
            );
        """
    )

    create_time_table = PostgresOperator(
        task_id='Create_time_table',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE IF NOT EXISTS time (
                start_time TIMESTAMP PRIMARY KEY,
                hour INT,
                day INT,
                week INT,
                month INT,
                year INT,
                weekday INT
            );
        """
    )

    # Create staging_events table
    create_staging_events_table = PostgresOperator(
        task_id='Create_staging_events_table',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE IF NOT EXISTS staging_events (
                event_id INT IDENTITY(0,1) PRIMARY KEY,
                artist VARCHAR,
                auth VARCHAR,
                firstName VARCHAR,
                gender VARCHAR,
                itemInSession INT,
                lastName VARCHAR,
                length FLOAT,
                level VARCHAR,
                location VARCHAR,
                method VARCHAR,
                page VARCHAR,
                registration FLOAT,
                sessionId INT,
                song VARCHAR,
                status INT,
                ts BIGINT,
                userAgent VARCHAR,
                userId INT
            );
        """
    )

    # Create staging_songs table
    create_staging_songs_table = PostgresOperator(
        task_id='Create_staging_songs_table',
        postgres_conn_id='redshift',
        sql="""
            CREATE TABLE IF NOT EXISTS staging_songs (
                song_id VARCHAR PRIMARY KEY,
                artist_id VARCHAR,
                artist_name VARCHAR,
                artist_location VARCHAR,
                artist_latitude FLOAT,
                artist_longitude FLOAT,
                title VARCHAR,
                duration FLOAT,
                year INT
            );
        """
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [create_songplays_table, create_users_table, create_songs_table, create_artists_table, create_time_table, create_staging_events_table, create_staging_songs_table] >> end_operator

create_tables_dag = create_tables_dag()
