from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.column = column
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {self.table}')
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        logging.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")

        null_records = redshift_hook.get_records(f"""
            SELECT COUNT(*) FROM {self.table}
            WHERE {self.column} IS NULL
        """)
        num_null_records = null_records[0][0]
        if num_null_records > 0:
            raise ValueError(f"""Data quality check failed. {self.table} contained
                            row(s) of NULL values in {self.column}""")


## Data Quality Operators
# The final operator to create is the data quality operator, which is used to run checks on the data itself.
# The operator's main functionality is to receive
#     - one or more SQL based test cases along with
#     - the expected results
# and execute the tests.
# For each test, the test result and expected result needs to be checked and
# if there is no match, the operator should raise an exception and the task should retry and fail eventually.
#
# For example one test could be a SQL statement that checks if certain column
# contains NULL values by counting all the rows that have NULL in the column.
# We do not want to have any NULLs so expected result would be 0 and the test
# would compare the SQL statement's outcome to the expected result.
