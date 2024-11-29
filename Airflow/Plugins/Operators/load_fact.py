from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        #log start of execution
        self.log.info(f"Loading data into fact table {self.table}")
        #create connection to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #Run the SQL INSERT command
        redshift.run(f"INSERT INTO {self.table} {self.sql}")

        #Log end of execution
        self.log.info(f"Data loaded successfully into fact table {self.table}")
