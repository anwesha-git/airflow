from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    staging_copy = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
		IGNOREHEADER {}
        DELIMITER '{}'
        REGION '{}'
        JSON '{}';
     """
	 
    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 aws_credentials_id="",
				 delimiter=",",
                 ignore_headers=1,
                 region="",
                 json_path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
		self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.region= region
        self.json_path = json_path        


    def execute(self, context):
        self.log.info("Executing StageToRedshiftOperator")
        self.log.info("Creating AWS Hook for Credentials")
		aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info("Creating Postgres SQL Hook for Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing old data from Staging Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.staging_copy.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter,
            self.region,
            self.json_path        
        )
        redshift.run(formatted_sql)
        self.log.info("Success: Loading data from S3 to Redshift")




