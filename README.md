# Data Pipeline Automation in Airflow

## Overview
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


## Check Connection to Airflow
--> I created a custome python script to instantiate airflow and create a connection
Check "initiate_airflow.py"


## Step 1
1. Create IAM User in AWS and add credentials to Airflow
2. Copy data from external s3 bucket, "udacity-dend" to personal bucket, "peter-automation-s3"

## Step 2
3. Configure variables in Airflow
4. Create/configure publicly accessible Redshift serverless
5. Add Airflow Connections to AWS Redshift

## Step 3
7. Create tables in Redshift using the create_tables_DAG.py file
8. Customize plugins/operators: data_quality.py, load_dimension.py, load_fact.py, and stage_redshift.py
   **StageToRedshiftOperator**: Copies JSON data from S3 to Redshift staging tables using the COPY command.
   **LoadFactOperator**: Inserts data from staging tables into the fact table.
   **LoadDimensionOperator**: Populates dimension tables with options for truncate-insert or append modes.
   **DataQualityOperator**: Runs validation checks on tables to ensure data consistency and correctness.


   
10. Write SQL scripts to populate the tables from staged data in Redshift (see Helpers/sql_statements.py)
11. Run the DAG to execute all the tasks:
    **stage_event**
    **stage_songs**
    **Load_songplays_fact_table**
    **Load_artist_dim_table**
    **Load_song_dim_table**
    **Load_time_dim_table**
    **Load_user_dim_table**
    **Run_data_quality_checks**
12. Monotor DAG run

## Summary
1. Configure AWS Credentials
2. Configure Redshift Connection
3. Update S3 and Redshift Details
4. Install Dependencies
5. Run the DAG
