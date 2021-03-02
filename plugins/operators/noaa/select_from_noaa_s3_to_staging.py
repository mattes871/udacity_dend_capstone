import os
from datetime import date, datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
#from hooks.s3_hook_local import S3HookLocal
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operators.download_s3_file_to_staging import DownloadS3FileToStagingOperator
from helpers.sql_queries import SqlQueries


class SelectFromNOAAS3ToStagingOperator(BaseOperator):
    """
    Select records for a specific day from the NOAA table file on Amazon S3.
    and write them onto the staging location as csv file.
    """

    ui_color = '#00FF00'
    template_fields=["execution_date",
                     "s3_table_file",
                     "most_recent_data_date",
                     "local_path"]

    @apply_defaults
    def __init__(self,
                 aws_credentials: str = '',
                 s3_bucket: str = '',
                 s3_prefix: str = '',
                 s3_table_file:  str = '',
                 most_recent_data_date: str = '',
                 execution_date: str = '',
                 real_date:  str = '',
                 local_path: str = '',
                 fact_delimiter: str = ',',
                 quotation_char: str = '"',
                 *args, **kwargs):

        super(SelectFromNOAAS3ToStagingOperator, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.s3_bucket=s3_bucket
        self.s3_prefix=s3_prefix
        self.s3_table_file=s3_table_file
        self.most_recent_data_date=most_recent_data_date
        self.execution_date=execution_date
        self.real_date=real_date
        self.local_path=local_path  # +"{{ execution_date.year }}"
        self.fact_delimiter=fact_delimiter
        self.quotation_char=quotation_char
        print(f"""SelectFromNOAAS3ToStagingOperator:
        {self.aws_credentials}
        {self.s3_bucket}
        {self.s3_prefix}
        {self.s3_table_file}
        {self.most_recent_data_date}
        {self.execution_date}
        {self.real_date}
        {self.local_path}
        {self.fact_delimiter}
        {self.quotation_char}
        """)


    def execute(self, context: dict) -> None:
        """
        Select all records for the **execution_date** day from the NOAA
        s3_table_file on Amazon S3 and store the records as a csv file on the
        local staging area under <local_path>/<real_date> If more than one csv
        file is created, all of them will be in the same directory  just
        different file name
        """

        def load_full_year(year: int, context: dict) -> None:
            """
            Uses DownloadS3FileToStagingOperator to download full *year* csv.gz
            file from the NOAA archive onto the Staging Area.
            """
            DownloadS3FileToStagingOperator(
                task_id='Copy_noaa_s3_fact_file_to_staging',
                aws_credentials=self.aws_credentials,
                s3_bucket=self.s3_bucket,
                s3_prefix='csv.gz',
                s3_key=f'{year}.csv.gz',
                local_path=self.local_path,
                replace_existing=True
                ).execute(context)


        self.log.info(f'Executing SelectFromNOAAS3ToStagingOperator ...')
        self.log.debug(f'Params:\nMost recent: {self.most_recent_data_date},'+
                      f'\nExecution date: {self.execution_date},'+
                      f'\nBucket & Table File: {self.s3_bucket} + {self.s3_table_file}'+
                      f'\nStaging Location: {self.local_path}')
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials)
        # Make sure path for local staging exists
        most_recent_data_year = datetime.strptime(self.most_recent_data_date,
                                                  '%Y-%m-%d').year
        execution_date_year = datetime.strptime(self.execution_date,
                                                '%Y-%m-%d').year
        if not os.path.exists(self.local_path):
            self.log.debug(f"Create Staging Location Folder '{self.local_path}'")
            os.makedirs(self.local_path)

        # Check if we need to backfill past years. In the NOAA
        # bucket, full years can be downloaded -> much more efficient
        # than backfilling day by day
        while most_recent_data_year < execution_date_year:
            self.log.info(f'Download full year: {most_recent_data_year}')
            load_full_year(most_recent_data_year, context)
            most_recent_data_year += 1

        # If most_recent_data_year == execution year and the
        # date is Jan 1st, download the whole file instead
        # of multiple selects
        if ((most_recent_data_year == execution_date_year) &
            (self.most_recent_data_date[4:10] == '-01-01')):
            self.log.info('Download full year: {most_recent_data_year}')
            load_full_year(most_recent_data_year, context)
        else:
            # Restrict to records that are newer than the 'most_recent'
            # record in the staging table
            where_clause = f"where s._2 >= '{self.most_recent_data_date}'"
            f_sql = f"""select s._1 as id,
                               s._2 as date_,
                               s._3 as element,
                               s._4 as data_value,
                               s._5 as m_flag,
                               s._6 as q_flag,
                               s._7 as s_flag,
                               s._8 as observ_time
                        from s3object s
                        {where_clause}
                        """
            self.log.debug(f_sql)
            result = s3_hook.select_key(
                key = f'{self.s3_prefix}/{self.s3_table_file}',
                bucket_name = f'{self.s3_bucket}',
                expression  = f_sql,
                expression_type = 'SQL',
                input_serialization = {
                    'CSV': {
                           'FileHeaderInfo': 'NONE',
                           'FieldDelimiter': ','
                           },
                    'CompressionType': 'GZIP'
                },
                output_serialization = {
                    'CSV': {
                            'FieldDelimiter': f',',
                           }
                    }
                )
            # Debug Output
            self.log.debug(f'Result length: {len(result)}')
            self.log.debug(f'Result[:512]: {result[:512]}')
            # Store results as csv file
            csv_file_name = os.path.join(self.local_path,
                                         f'{self.execution_date}')+'.csv'
            self.log.debug(f'Type of result: {type(result)}')
            self.log.debug(f'Write to file: {csv_file_name}')
            with open(csv_file_name,'w') as f:
                #f.write(f'id,date_,element,data_value,m_flag,q_flag,s_flag,observ_time\n')
                f.write(f'{result}')

        self.log.info(f'SelectFromNOAAS3ToStagingOperator successful.')
