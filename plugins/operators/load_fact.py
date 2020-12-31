from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    facts_sql_template = """
    CREATE TABLE IF NOT EXISTS {destination_table} AS
    {fact_table_query}
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table_query="",
                 destination_table="",
                 *args, **kwargs):

        # this super(<SameClass>, self) call below is equivalent to
        # the parameterless super() call, giving access to methods
        # in a superclass from the subclass that inherits from it.
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.fact_table_query=fact_table_query


    def execute(self, context):
        # Fetch the redshift hook
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Format the `facts_sql_template`
        formatted_facts_sql = LoadFactOperator.facts_sql_template.format(
            destination_table=self.destination_table,
            fact_table_query=self.fact_table_query
        )

        # run the query against redshift
        redshift.run(formatted_facts_sql)
