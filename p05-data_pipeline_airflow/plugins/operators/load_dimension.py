from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''
    Loads dimension tables from staging tables into Redshift.
    '''

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 table = '',
                 sql = '',
                 append = False,
                 primary_key = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql
        self.append = append
        self.primary_key = primary_key

    def execute(self, context):
        self.log.info('Retrieve connection credentials')
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id

        # Check if we're appending data during insert (remove old inserts)
        if self.append:
            sql_insert = f'''
                create temp table stage_{self.table} (like {self.table}); 
                
                insert into stage_{self.table}
                {self.select_sql};
                
                delete from {self.table}
                using stage_{self.table}
                where {self.table}.{self.primary_key} = stage_{self.table}.{self.primary_key};
                
                insert into {self.table}
                select * from stage_{self.table};
            '''
        else:
            sql_insert = f'''
                insert into {self.table}
                {self.select_sql}
            '''
            
            self.log.info(f'Clear data from dimension table {self.table}')
            redshift_hook.run(f'TRUNCATE TABLE {self.table};')

        self.log.info('Load data into Redshift dimension table')
        redshift_hook.run(sql_insert)
