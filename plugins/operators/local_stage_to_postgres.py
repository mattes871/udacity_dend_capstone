import os
import glob
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LocalStageToPostgresOperator(BaseOperator):
    ui_color = '#358140'
    #template_fields=("s3_key",)
    copy_sql = """
                BEGIN;
                COPY {}
                FROM '{}'
                DELIMITER '{}'
                CSV HEADER ;
                END;
                """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 table='',
                 delimiter='',
                 truncate_table=True,
                 local_path='',
                 *args, **kwargs):

        super(LocalStageToPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.delimiter = delimiter
        self.truncate_table = truncate_table
        self.local_path=local_path

    def execute(self, context):
        """
          Copy csv data from local staging to postgres
        """

        def get_all_csv_files(path):
            """ Return a list containing all *.csv files
                from self.local_path """
            all_csv_files = glob.glob(os.path.join(path,'*.csv'))
            return all_csv_files

        def insert_csv_data_into_postgres( csv_file ):
            """ Use COPY to bulk-insert all records form *csv_file*
                into postgres table """
            f_sql = LocalStageToPostgresOperator.copy_sql.format(
                self.table,
                os.path.join('/var/lib/postgresql',csv_file),
                self.delimiter
            )
            self.log.info(f'Execute SQL: \n{f_sql}')
            result = postgres.run(f_sql)
            self.log.info(f'Result: {result}')
            return result


        self.log.info(f'Run LocalStageToPostgres Operator')
        postgres = PostgresHook(self.postgres_conn_id)
        # 1. On truncate_table delete all existing data from postgresi table
        #    TRUNCATE TABLE is faster than DELETE FROM but does not allow any rollback
        if self.truncate_table:
            self.log.info(f'Delete data from postgres table {self.table}')
            #postgres.run(f'DELETE FROM {self.table}')
            postgres.run(f'TRUNCATE TABLE {self.table}')

        all_csv_files = get_all_csv_files(self.local_path)
        for csv_file in all_csv_files:
            self.log.info(f'Insert file {csv_file} into Postgres')
            insert_csv_data_into_postgres(csv_file)

        self.log.info('LocalStageToPostgresOperator successful')
