from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToPostgresOperator(BaseOperator):
    ui_color = '#358140'
    template_fields=("s3_key",)
    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                REGION '{}'
                TIMEFORMAT as 'epochmillisecs'
                TRUNCATECOLUMNS
                FORMAT AS CSV DELIM '{}'
                """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 delimiter=';',
                 truncate_table=False,
                 *args, **kwargs):

        super(StageToPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.delimiter = delimiter
        self.truncate_table = truncate_table

    def execute(self, context):
        """
          Copy S3 data to postgres
        """

        self.log.info(f'Copy data from S3 to postgres')
        # Use Hook to connect to Amazon S
        self.log.info(f'AwsHook({self.aws_credentials_id},{self.region})')
        aws_hook = AwsHook(self.aws_credentials_id, self.region)
        credentials = aws_hook.get_credentials()
        postgres = PostgresHook(self.postgres_conn_id)
        # 1. On truncate_table delete all existing data from postgres
        #    TRUNCATE TABLE is faster than DELETE FROM but does not allow any rollback
        if self.truncate_table:
            self.log.info(f'Delete data from postgres table {self.table}')
            postgres.run(f'DELETE FROM {self.table}')

        # 2. Specify s3_path: s3_key can be jinja templated
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        self.log.info(f'S3 path for copy is {s3_path}')

        # 3. Copy data from S3 to postgres
        self.log.info(f'Copy data from S3 {s3_path} to postgres')
        f_sql = StageToPostgresOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.delimiter
        )
        self.log.info(f'Executing StageToPostgresOperator: \n{f_sql}')
        postgres.run(f_sql)
        self.log.info('StageToPostgresOperator successful')
