import os
import glob
import gzip, shutil
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LocalStageToPostgresOperator(BaseOperator):
    ui_color = '#358140'
    #template_fields=("s3_key",)
    copy_sql = """
                BEGIN;
                COPY {}
                FROM stdin
                WITH DELIMITER '{}'
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

    def execute(self, context: dict) -> None:
        """
          Copy csv data from local staging to postgres
        """

        def get_all_pattern_files(path: str, pattern: str) -> list:
            """ Return a list containing all *.csv files
                from self.local_path 
                *pattern* could be e.g. '*.csv'
            """
            all_csv_files = glob.glob(os.path.join(path,pattern))
            return all_csv_files

        def insert_csv_data_into_postgres(postgres: PostgresHook,
                                          csv_file: str,
                                          gzipped: bool ) -> any:
            """ Use COPY to bulk-insert all records from
                local *csv_file* into postgres table """
            f_sql = LocalStageToPostgresOperator.copy_sql.format(
                self.table,
                self.delimiter
            )
            self.log.info(f'Execute SQL: \n{f_sql}')
            # Unzip file to temporary location if gzipped
            if gzipped:
                self.log.info(f'Unzipping {csv_file}')
                with gzip.open(csv_file, 'rb') as f_in:
                    with open('tmp.csv', 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                csv_file = 'tmp.csv'
            # copy_expert to import from a local file
            self.log.info(f'Importing from {csv_file}')
            result = postgres.copy_expert(f_sql, csv_file)
            # If file was unzipped to a temp file, remove the temp file
            if gzipped:
                self.log.info(f'Removing tmp.csv')
                os.remove('tmp.csv')
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

        all_csv_files = get_all_pattern_files(self.local_path,'*.csv')
        all_gz_csv_files =  get_all_pattern_files(self.local_path,'*.csv.gz')
        for csv_file in all_csv_files:
            self.log.info(f'Insert file {csv_file} into Postgres')
            insert_csv_data_into_postgres(postgres,csv_file,gzipped=False)
        for csv_file in all_gz_csv_files:
            self.log.info(f'Insert file {csv_file} into Postgres')
            insert_csv_data_into_postgres(postgres,csv_file,gzipped=True)


        self.log.info('LocalStageToPostgresOperator successful')
