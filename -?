from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_query='',
                 truncate_table=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):
        """
          Load dimensional information and insert this into corresponding Redshift table.
          If 'truncate_table' is True, all existing records are deleted from the table
          before inserting new dimensional data
        """

        self.log.info(f'Executing LoadDimensionOperator with query:\n{self.sql_query}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        ## Delete all existing records
        if self.truncate_table:
            self.log.info(f'Deleting existing records from {self.table}')
            redshift.run(f'DELETE FROM {self.table}')
        ## Insert new records
        redshift.run(f'Insert into {self.table} {self.sql_query}')
        self.log.info(f'Succeeded with LoadDimensionOperator')
