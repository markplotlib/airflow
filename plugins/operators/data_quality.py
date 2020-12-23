from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table="",
                 column="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.column = column
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # Fetch the redshift hook
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # query COUNT(*)
        records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {self.table}')

        # verify COUNT(*) > 0
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        logging.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")

        # checks if certain column contains NULL values
        # by counting all the rows that have NULL in the column.
        null_records = redshift_hook.get_records(f"""
            SELECT COUNT(*) FROM {self.table}
            WHERE {self.column} IS NULL
        """)
        num_null_records = null_records[0][0]
        if num_null_records > 0:
            raise ValueError(f"""Data quality check failed. {self.table} contained
                            row(s) of NULL values in {self.column}""")
