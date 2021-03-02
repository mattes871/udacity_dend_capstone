import os
from datetime import datetime, timedelta
from airflow.models import Variable

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GetMostRecentDataDateOperator(BaseOperator):
    """
    Get the date of the most recent fact in the specified
    {schema}.{table} using timestamp or date-type field {date_}
    from postgres and set a variable with the result in airflow
    """

    ui_color = '#999966'
    sql_cmds = """SELECT to_char(max({}),'YYYY-MM-DD') as max_date_str
                  FROM {}.{}
                  WHERE {} ;
                  """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id: str,
                 schema: str,
                 table: str,
                 date_field: str,
                 where_clause: str,
                 airflow_var_name: str,
                 *args, **kwargs):

        super(GetMostRecentDataDateOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.table = table
        self.where_clause = where_clause
        self.date_field = date_field
        self.airflow_var_name = airflow_var_name

    def execute(self, context: dict) -> None:
        """
        Read sql_query_file and execute the SQL code in Postgresql
        The content of the sql file is enclosed in a 'BEGIN/END',
        so that the file can contain multiple statements that will
        be executed sequentially.
        """

        # Get date of most recent data from production table
        postgres = PostgresHook(self.postgres_conn_id)
        connection = postgres.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql_cmds.format(self.date_field, self.schema,
                                       self.table, self.where_clause))
        most_recent = cursor.fetchone()
        try:
            most_recent_date = datetime.strptime(most_recent[0],'%Y%m%d')
        except:
            most_recent_date = NOAA_DATA_AVAILABLE_FROM
        else:
            # Add one day to avoid complications with
            # Dec 31st dates
            most_recent_date += timedelta(days=1)
        self.log.info(f"Most recent data in '{self.schema}.{self.table}' is as of: {most_recent_day}")
        Variable.delete(f'{self.airflow_var_name}')
        Variable.set(f'{self.airflow_var_name}', most_recent_date) #.strftime('%Y%m%d'))
