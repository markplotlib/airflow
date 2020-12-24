# notes: StageToRedshiftOperator
#   load JSON files from S3 to Redshift; create and run SQL COPY statement
#   parameters specify where in S3 the file is loaded, and the target table
#   parameters to distinguish between JSON file
#   stage operator also contains templated field that allows it to load
#   timestamped files from S3 based on the execution time and run backfills.

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
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id


    def execute(self, context):
        # instantiate AwsHook() object
        aws_hook = AwsHook(self.aws_credentials_id)
        # assign credentials
        credentials = aws_hook.get_credentials()

        # instantiate PostgresHook() object with postgres_conn_id=redshift_conn_id
        redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

        self.log.info('Clearing data from destination Redshift table')
        # TODO: run SQL DELETE command

        self.log.info('Copying data from S3 to Redshift')
        # TODO: run SQL COPY command
            # TODO: set rendered_key from context
            # TODO: set s3_path, from s3_bucket and from rendered_key

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            # s3_path=,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            ignore_header=self.ignore_header,
            delimiter=self.delimiter
        )
        redshift.run(formatted_sql)
