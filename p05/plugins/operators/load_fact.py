from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''
    Loads fact table from staging tables into Redshift.
    '''
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 table = '',
                 sql = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info('Retrieve connection credentials')
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info('Loading data into fact table in Redshift')
        sql_insert = f'''
            INSERT INTO {self.table}
            {self.sql}
        '''
        redshift_hook.run(sql_insert)
