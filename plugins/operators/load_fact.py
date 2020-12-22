from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    facts_sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    {songplay_table_insert}
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 songplay_table_insert="",
                 origin_table="",
                 destination_table=""
                 *args, **kwargs):

        # this super(<SameClass>, self) call below is equivalent to
        # the parameterless super() call, giving access to methods
        # in a superclass from the subclass that inherits from it.
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.origin_table=origin_table
        self.destination_table=destination_table
        self.songplay_table_insert=songplay_table_insert


    def execute(self, context):
        # Fetch the redshift hook
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Format the `facts_sql_template`
        formatted_facts_sql = LoadFactOperator.facts_sql_template.format(
            destination_table=self.destination_table,
            origin_table=self.origin_table,
            songplay_table_insert=self.songplay_table_insert
        )

        # run the query against redshift
        redshift.run(formatted_facts_sql)
