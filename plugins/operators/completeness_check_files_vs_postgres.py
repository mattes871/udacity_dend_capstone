import os
import glob
import gzip, shutil
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CompletenessCheckFilesVsPostgresOperator(BaseOperator):
    """
    Check a (set of) csv or other text files for which each line should
    translate to a new record in the Postgres staging tables.
    The Operator throws an AssertionError if the number of
    lines does not correspond to the number of records in the given table.
    """

    ui_color = '#006666'
    template_fields=("local_path",)
    count_sql = """
                SELECT count(1) FROM {}.{} ;
                """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id: str = '',
                 schema:       str = '',
                 table:        str = '',
                 local_path:   str = '',
                 file_pattern: str = '*.csv',
                 header_line: bool = False,
                 *args, **kwargs):

        super(CompletenessCheckFilesVsPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema,
        self.table = table
        self.local_path, = local_path,
        self.header_line = header_line
        self.file_pattern, = file_pattern,

    def execute(self, context: dict) -> None:
        """
        xxx
        """

        # Source for https://stackoverflow.com/questions/845058/how-to-get-line-count-of-a-large-file-cheaply-in-python
        def file_len(fname):
            """
            Get number of lines of a text file
            """
            i=-1
            with open(fname) as f:
                for i, l in enumerate(f):
                    pass
            if self.header_line:
                return i
            else:
                return i+1

        def get_all_pattern_files(path: str, pattern: str) -> list:
            """ Return a list containing all *.csv files
                from self.local_path
                *pattern* could be e.g. '*.csv'
            """

            all_csv_files = glob.glob(os.path.join(path,pattern))
            return all_csv_files


        self.log.debug(f"Run LocalCSVToPostgresOperator({self.table},\n"+
                      f"    '{self.delimiter}',\n"+
                      f"    {self.local_path},\n"+
                      f"    {self.file_pattern}")
        # Get the number of lines from the text files
        text_files = get_all_pattern_files(self.local_path, self.file_pattern)
        self.log.info(f"Found {len(text_files)} files for import.")
        total_lines_in_text_files = -1
        for text_file in text_files:
            total_lines_in_text_files += file_len(postgres, text_file)

        # Now count the number of records in the given table
        postgres = PostgresHook(self.postgres_conn_id)
        connection = postgres.get_conn()
        cursor = connection.cursor()
        cursor.execute(LocalCSVToPostgresOperator.count_sql \
                            .format(self.schema, self.table))
        result = cursor.fetchone()
        number_of_records = bigint(result[0])

        # Throw an AssertionError if the lines in the files and records 
        # in the table are different
        assert (total_lines_in_text_files == number_of_records), f"Number of lines in '{self.file_pattern}' is {total_lines_in_text_files} but number of records in '{self.schema}.{self.table}' is {number_of_records}!"
