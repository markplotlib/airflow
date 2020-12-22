from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    dim_sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    {dim_table_query}
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dim_table_query="",
                 destination_table=""
                 *args, **kwargs):

        # this super(<SameClass>, self) call below is equivalent to
        # the parameterless super() call, giving access to methods
        # in a superclass from the subclass that inherits from it.
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.dim_table_query=dim_table_query
        

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
