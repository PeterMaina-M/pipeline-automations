from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON {} 
        REGION '{}'
        TIMEFORMAT 'auto'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format='auto',
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format
        self.region = region

    def execute(self, context):
        #Log task start
        self.log.info(f"Starting data staging for table {self.table}")
        
        #Retrieve AWS credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        #Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #Clear existing data
        self.log.info("Clearing data from destination {}".format(self.table))
        redshift.run(f"DELETE FROM {self.table}")

        #Determining JSON format
        json_format = "'auto'" if self.file_format == "auto" else f"'{self.file_format}'"

        #Run COPY command
        self.log.info("Copying data from S3 to {}".format(self.table))
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            json_format,
            self.region
        )
        self.log.info("Running COPY command on Redshift")
        redshift.run(formatted_sql)

        #Log END of task
        self.log.info(f"Copying complete for {self.table}")


       
