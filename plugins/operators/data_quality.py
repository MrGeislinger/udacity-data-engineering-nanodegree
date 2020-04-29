from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''
    Run quality check on data with a test query.
    '''
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 test_query = '',
                 test_result = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.test_query = test_query
        self.test_result = test_result

    def execute(self, context):
        self.log.info('Retrieve connection credentials')
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info('Run test query')
        records = redshift_hook.get_records(self.test_query)
        if records[0][0] != self.test_result:
            raise ValueError(f'FAILED data quality check: {records[0][0]} does not equal {self.test_result}')
        else:
            self.log.info('PASSED: Data quality check')