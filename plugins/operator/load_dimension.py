from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_query="",
                 truncate="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        self.log.info("Executing LoadDimensionOperator")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info("Truncate table {}".format(self.table))
            redshift_hook.run("TRUNCATE {}".format(self.table))
        self.log.info("Loading the dimension table {}".format(self.table))
        redshift_hook.run("INSERT INTO {} {}".format ((self.table),(self.sql_query)))
        self.log.info("Success: Loading the fact table")
        pass