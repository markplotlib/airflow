# notes: StageToRedshiftOperator
#   load JSON files from S3 to Redshift; create and run SQL COPY statement
#   parameters specify where in S3 the file is loaded,
#   and the target table parameters to distinguish between JSON file
#   The stage operator also contains a templated field that allows it to load
#   timestamped files from S3 based on the execution time and run backfills.

# from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        IGNOREHEADER {ignore_header}
        DELIMITER '{delimiter}'
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id


    def execute(self, context):
        # instantiate S3Hook() object (instead of AwsHook)
        s3hook = S3Hook(self.aws_credentials_id)
        # assign credentials
        credentials = s3hook.get_credentials()

        # instantiate PostgresHook() object with postgres_conn_id=redshift_conn_id
        redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

        self.log.info('Clearing data from destination Redshift table')
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            ignore_header=self.ignore_header,
            delimiter=self.delimiter
        )
        redshift.run(formatted_sql)
