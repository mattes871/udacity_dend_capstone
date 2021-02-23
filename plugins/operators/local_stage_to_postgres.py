import os
import glob
import gzip, shutil
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LocalStageToPostgresOperator(BaseOperator):
    """
    Import a (set of) csv files into an existing(!) table in a Postgresql
    database. Depending on the parameters, the table can be emptied before the
    import. The csv file(s) can be plain text or gzipped.
    """
    
    ui_color = '#00FF99'
    #template_fields=("s3_key",)
    copy_sql = """
                BEGIN;
                COPY {}
                FROM stdin
                WITH DELIMITER '{}'
                {}
                CSV HEADER ;
                END;
                """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id: str = '',
                 table:        str = '',
                 delimiter:    str = '',
                 quote:        str = '',
                 truncate_table: bool = True,
                 local_path:   str = '',
                 file_pattern: str = '*.csv',
                 gzipped: bool = False,
                 *args, **kwargs):

        super(LocalStageToPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.delimiter = delimiter
        self.quote = quote
        self.truncate_table = truncate_table
        self.local_path, = local_path,
        self.file_pattern, = file_pattern,
        self.gzipped = gzipped

        print(f"""LocalStageToPostgresOperator:
        {self.postgres_conn_id}
        {self.table}
        {self.delimiter}
        {self.quote}
        {self.truncate_table}
        {self.local_path}
        {self.file_pattern}
        {self.gzipped}""")


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

        def import_csv_data_into_postgres(postgres: PostgresHook,
                                          csv_file: str) -> any:
            """ Use COPY to bulk-insert all records from
                local *csv_file* into postgres table """

            # Insert QUOTE '' statement if quotation character is given
            if self.quote != '':
                quote_str = f"QUOTE '{self.quote}'"
            else:
                quote_str = ''
            f_sql = LocalStageToPostgresOperator.copy_sql.format(
                self.table,
                self.delimiter,
                quote_str
            )
            self.log.info(f'Execute SQL: \n{f_sql}')
            # Unzip file to temporary location if gzipped
            if self.gzipped:
                self.log.info(f'Unzipping {csv_file}')
                with gzip.open(csv_file, 'rb') as f_in:
                    with open('tmp.csv', 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                csv_file = 'tmp.csv'
            # copy_expert to import from a local file
            self.log.info(f'Importing from {csv_file}')
            result = postgres.copy_expert(f_sql, csv_file)
            # If file was unzipped to a temp file, remove the temp file
            if self.gzipped:
                self.log.info(f'Removing tmp.csv')
                os.remove('tmp.csv')
            self.log.info(f'Result: {result}')
            return result


        self.log.info(f"Run LocalStageToPostgresOperator({self.table},\n"+
                      f"    '{self.delimiter}',\n"+
                      f"    {self.local_path},\n"+
                      f"    {self.file_pattern},\n"+
                      f"    {self.gzipped})")

        postgres = PostgresHook(self.postgres_conn_id)

        # On truncate_table delete all existing data from postgres table
        # TRUNCATE TABLE is faster than DELETE FROM but does not allow any rollback
        if self.truncate_table:
            self.log.info(f'Delete data from postgres table {self.table}')
            #postgres.run(f'DELETE FROM {self.table}')
            postgres.run(f'TRUNCATE TABLE {self.table}')

        csv_files = get_all_pattern_files(self.local_path, self.file_pattern)

        for csv_file in csv_files:
            self.log.info(f'Import file {csv_file} into Postgres using copy_from')
            import_csv_data_into_postgres(postgres, csv_file)


