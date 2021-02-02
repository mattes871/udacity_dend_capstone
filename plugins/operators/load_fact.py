from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        """
          Load fact information and insert this into corresponding Redshift table.
          All existing records are deleted from the table before inserting
          new fact data
        """

        self.log.info(f'Executing LoadFactOperator with query:\n{self.sql_query}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        ## Delete all existing records
        redshift.run(f'DELETE FROM {self.table}')
        ## Insert new records
        redshift.run(f'Insert into {self.table} {self.sql_query}')
        self.log.info(f'Succeeded with LoadFactOperator')
