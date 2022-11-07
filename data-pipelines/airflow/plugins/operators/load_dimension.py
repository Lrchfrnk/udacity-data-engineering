from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

#    load_dimension_table_insert = "INSERT INTO {} {}"
 #   load_dimension_table_truncate = "TRUNCATE TABLE {}"
    dimension_table_template = """
        DROP TABLE IF EXISTS {target_table}
        CREATE TABLE IF NOT EXISTS {target_table} AS {insert_sql}
    """

    
    @apply_defaults
    def __init__(self,
                 query="",
                 redshift_conn_id="",
                 target_table="",
                 insert_sql="",
                 *args, **kwargs):
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.query=query
        self.redshift_conn_id=redshift_conn_id
        self.target_table=target_table
        self.insert_sql=insert_sql
        
    def execute(self, context):
        self.log.info(f"Creating table for dimension table {self.target_table} ")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
       
    
        redshift.run(LoadDimensionOperator.dimension_table_template.format(self.t_name, self.query)) 
        
        fact_sql = LoadDimensionOperator.load_dimension_table_insert.format(target_table=self.target_table, insert_sql = self.insert_sql)
        redshift.run(fact_sql)

        self.log.info(f"Creation of table {self.target_table} done")