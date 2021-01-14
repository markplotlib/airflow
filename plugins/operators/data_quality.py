from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_names=[],
                 column="",
                 quality_checks=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_names = table_names
        self.column = column
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks: list = quality_checks


    def execute(self, context):
        # Fetch the redshift hook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("executing Data Quality query")
        # query COUNT(*)
        for table in self.table_names:
            unformatted_sql = quality_checks[0]['check_sql'].format(table)
            formatted_sql = unformatted_sql.format(table)
            record_count = redshift_hook.get_records(formatted_sql)
            # wrong method?  record_count = redshift_hook.run(formatted_sql)
            # verify COUNT(*) > 0
            if record_count < quality_checks[0]['fail_result']):
                self.log.info("Data quality check failed. {} returned no results".format(table))

        self.log.info("Data Quality check successfully completed.")

            # wrong code?
            # if (len(record_count) < quality_checks[0]['fail_result'] or len(record_count[0]) < quality_checks[0]['fail_result']):
            #     self.log.info("Data quality check failed. {} returned no results".format(table))
