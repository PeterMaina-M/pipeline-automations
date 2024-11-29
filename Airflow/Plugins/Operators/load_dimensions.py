from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    truncate_sql = """
        TRUNCATE TABLE {};
    """

    insert_sql = """
        INSERT INTO {} {};
    """


    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                 table="",
                 sql="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    
    def execute(self, context):
        #Log start of execution
        self.log.info(f"Starting to load data into dimension table {self.table}")
        
        #Connect with Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #If statement to truncate before inserting
        if self.truncate:
            self.log.info(f"Truncating table {self.table} in Redshift")
            redshift_hook.run(LoadDimensionOperator.truncate_sql.format(self.table))

        #Insert data into the dimension table
        self.log.info(f"Inserting data into {self.table} dimension table in Redshift")
        formatted_sql = LoadDimensionOperator.insert_sql.format(self.table, self.sql)
        
        self.log.info(f"Executing SQL: {formatted_sql}")
        redshift_hook.run(formatted_sql)

        #Log end of execution
        self.log.info(f"Data load into {self.table} dimension table complete")
