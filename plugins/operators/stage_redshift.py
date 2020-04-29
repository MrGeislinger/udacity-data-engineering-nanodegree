from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    '''
    Stages Redshift tables from JSON data found in S3 buckets.
    '''
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, aws_credentials_id='', table='', s3_bucket='', s3_key='',
                 copy_json_option='auto', region='', conn_id='', *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region

    def execute(self, context):
        self.log.info('Retrieve AWS credentials')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Removing data from Redshift')
        redshift.run(f'DELETE FROM {self.table}')
        
        self.log.info('Copying data from S3 bucket to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'

        # Copy from s3 bucket
        sql = f'''
                COPY {self.table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                REGION AS '{self.region}'
                FORMAT as json '{self.copy_json_option}'
        '''
        redshift.run(sql)





