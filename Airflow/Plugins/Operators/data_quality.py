from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults) here
                 redshift_conn_id="",
                 tests=[], #this is here to help pass two conditions song_data and log_data
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        # Log start of execution
        self.log.info("Starting data quality checks")
        # Connect to Redshift using PostgresHook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Iterate over the list of tests
        for test in self.tests:
            sql_query = test.get("check_sql")
            expected_result = test.get("expected_result")

            # Log the SQL being executed
            self.log.info(f"Running SQL: {sql_query}")

            # Execute the SQL query and fetch the result
            records = redshift_hook.get_records(sql_query)

            if not records or len(records[0]) == 0:
                # If there's no result returned, raise an error
                self.log.error(f"No results returned from query: {sql_query}")
                raise ValueError(f"Data quality check failed. Query did not return any results: {sql_query}")

            # Fetch the actual result from the query
            actual_result = records[0][0]
            self.log.info(f"Expected result: {expected_result}, Actual result: {actual_result}")

            # Check if the actual result matches the expected result
            if actual_result != expected_result:
                # Log the mismatch and raise an exception
                self.log.error(f"Data quality check failed. Expected: {expected_result}, Got: {actual_result}")
                raise ValueError(f"Data quality check failed for query: {sql_query}")
            else:
                # Log success if the result matches
                self.log.info(f"Data quality check passed for query: {sql_query}")

        # Log end of execution
        self.log.info("Data quality checks completed successfully")
