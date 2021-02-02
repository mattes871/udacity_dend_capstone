from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 dq_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        """
          Execute the data quality checks provided in 'dq_checks'
          Each entry is a dictionary with key-value pairs for
          'sql': SQL string whose execution delivers a number
          'expected': True, if the 'value' (see below) describes the expected result (=pass)
                      False, if 'value' indicates an error
          'value': The value the sql-result is compared against
        """

        self.log.info(f'Executing DataQualityOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for dq_check in self.dq_checks:
            sql = dq_check['sql']
            expected = dq_check['expected']
            value = dq_check['value']
            records = redshift.get_records(sql)
            num_records = records[0][0]
            if (expected and num_records != value):
                raise ValueError(f'Data quality check failed. Expected: {value} | Got: {num_records}')
            elif (not expected and num_records == value):
                raise ValueError(f'Data quality check failed. Got: {num_records}')
            else:
                self.log.info(f'Data quality on SQL {sql} check passed with {records[0][0]} records')
        self.log.info('All data quality checks passed without error.')
