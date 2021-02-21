from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    """ Create Tables on Postgresql for
        loading the data from S3 into
        a staging area
    """

    ui_color = '#FFFFFF'
    sql_cmds = """BEGIN ;
                  {}
                  END ;
                  """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 sql_query_file='',
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_query_file = sql_query_file
        self.log.info(f'Init CreateTablesOperator({postgres_conn_id},{sql_query_file})')

    def execute(self, context):
        """
          Read sql_query_file and execute the SQL code in Postgresql
          The content of the sql file is enclosed in a 'BEGIN/END',
          so that the file can contain multiple statements that will
          be executed sequentially.
        """
        self.log.info(f'Executing CreateTablesOperator with file:\n{self.sql_query_file}')
        with open(self.sql_query_file, mode='r') as f:
            sql_query = f.read()
            # f.close() happens implicitly when exiting with-scope
        sql_query = CreateTablesOperator.sql_cmds.format(sql_query)
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info(f'Executing CreateTablesOperator with:\n{sql_query}')
        postgres.run(sql_query)
        self.log.info(f'Succeeded with CreateTablesOperator from file:\n{self.sql_query_file}')

