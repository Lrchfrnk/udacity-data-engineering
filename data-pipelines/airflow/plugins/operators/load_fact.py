from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    fact_table_template = """
        DROP TABLE IF EXISTS {target_table}
        CREATE TABLE IF NOT EXISTS {target_table} AS {insert_sql}
    """

    @apply_defaults
    def __init__(self, 
                 redshift_conn_id="", 
                 target_table="",
                 insert_sql ="",
                 *args, 
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.insert_sql = insert_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        fact_sql = LoadFactOperator.fact_table_template.format(target_table=self.target_table, insert_sql = self.insert_sql)
        redshift.run(fact_sql)
        self.log.info('LoadFactOperator has been executed') 
