from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    sql_template = ""
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dim_table_query="",
                 destination_table="",
                 append_records=True,
                 *args, **kwargs):

        # this super(<SameClass>, self) call below is equivalent to
        # the parameterless super() call, giving access to
        # methods in a superclass to the inheriting subclass.
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.dim_table_query=dim_table_query
        self.append_records=append_records


    def execute(self, context):
        # Fetch the redshift hook
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        if append_records:
            sql_template = """
            INSERT INTO TABLE {destination_table} AS
            {dim_table_query}
            """
        else:
            sql_template = """
            DROP TABLE IF EXISTS {destination_table};
            CREATE TABLE {destination_table} AS
            {dim_table_query}
            """
            self.log.info("Deleting records from existing Redshift table")

        # Format the SQL template
        formatted_sql = LoadDimensionOperator.sql_template.format(
            destination_table=self.destination_table,
            dim_table_query=self.dim_table_query
        )

        # run the query against redshift
        redshift.run(formatted_sql)

        self.log.info("Loading dimension table")
