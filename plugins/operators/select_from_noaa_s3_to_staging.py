import os
from datetime import date, datetime
#from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from hooks.s3_hook_local import S3HookLocal
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operators.copy_noaa_s3_files_to_staging import CopyNOAAS3FilesToStagingOperator

class SelectFromNOAAS3ToStagingOperator(BaseOperator):
    """ Select records for a specific day from the
        NOAA table file on Amazon S3.
        and write them onto the staging location as 
        csv file.
    """

    ui_color = '#00FF00'
    template_fields=["execution_date",
                     "s3_table_file",
                     "most_recent_data_date",
                     "local_path"]

    @apply_defaults
    def __init__(self,
                 aws_credentials='',
                 s3_bucket='',
                 s3_prefix='',
                 s3_table_file='',
                 most_recent_data_date='',
                 execution_date='',
                 real_date='',
                 local_path='',
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

    def execute(self, context: dict) -> None:
        """ Select all records for the **execution_date** day from the
            NOAA s3_table_file on Amazon S3 and store the records
            as a csv file on the local staging area under
            <local_path>/<real_date>
            If more than one csv file is created, all of them will be
            in the same directory  just different file name
        """


        def load_full_year(year: int, context: dict) -> None:
            """ Uses CopyNOAAS3FileToStagingOperator to download
                full *year* csv.gz file from the NOAA archive onto
                the Staging Area
            """
            CopyNOAAS3FilesToStagingOperator(
                task_id='Copy_noaa_s3_fact_file_to_staging',
                aws_credentials=self.aws_credentials,
                s3_bucket=self.s3_bucket,
                s3_prefix='csv.gz',
                s3_files=[f'{year}.csv.gz'],
                local_path=self.local_path
                ).execute(context)


        self.log.info(f'Executing SelectFromNOAAS3ToStagingOperator ...')
        self.log.info(f'Params:\nMost recent: {self.most_recent_data_date},'+
                      f'\nExecution date: {self.execution_date},'+
                      f'\nBucket & Table File: {self.s3_bucket} + {self.s3_table_file}'+
                      f'\nStaging Location: {self.local_path}')
        s3_hook = S3HookLocal(aws_conn_id=self.aws_credentials)
        # Make sure path for local staging exists
        most_recent_data_year = datetime.strptime(self.most_recent_data_date,
                                                  '%Y%m%d').year
        execution_date_year=datetime.strptime(self.execution_date,
                                              '%Y%m%d').year
        if not os.path.exists(self.local_path):
            self.log.info(f'Create Staging Location Folder')
            os.makedirs(self.local_path)

        # Check if we need to backfill past years. In the NOAA
        # bucket, full years can be downloaded -> much more efficient
        # than backfilling day by day
        while most_recent_data_year < execution_date_year:
            self.log.info(f'Download full year: {most_recent_data_year}')
            load_full_year(most_recent_data_year, context)
            most_recent_data_year += 1

        # If most_recent_data_year == execution year and the
        # date is Jan 1st, lets download the whole file instead
        # of multiple selects
        if ((most_recent_data_year == execution_date_year) &
            (self.most_recent_data_date[4:8] == '0101')):
            self.log.info('Download full year: {most_recent_data_year}')
            load_full_year(most_recent_data_year, context)
        else:
        # Restrict to records that are newer than the 'most_recent'
        # record in the staging table
        #
        # >>>> WIP: What if 'most_recent' is in past year
        # >>>>      records need to be loaded from different file
        #
            where_clause = f"where s._2 >= '{self.most_recent_data_date.strftime('%Y%m%d')}'"
            self.log.info(f"""select s._1 as id,
                                      s._2 as date_,
                                      s._3 as element,
                                      s._4 as data_value,
                                      s._5 as m_flag,
                                      s._6 as q_flag,
                                      s._7 as s_flag,
                                      s._8 as observ_time
                               from s3object s
                               {where_clause}
                               limit 32""")
            result = s3_hook.select_key(
                key=f'{self.s3_prefix}/{self.s3_table_file}',
                bucket_name=f'{self.s3_bucket}',
                expression=f"""select s._1 as id,
                                      s._2 as date_,
                                      s._3 as element,
                                      s._4 as data_value,
                                      s._5 as m_flag,
                                      s._6 as q_flag,
                                      s._7 as s_flag,
                                      s._8 as observ_time
                               from s3object s
                               {where_clause}
                               limit 32""",
                input_serialization={
                    'CSV': {
                           'FileHeaderInfo': 'NONE',
                           'FieldDelimiter': ','
                        },
                    'CompressionType': 'GZIP'
                    }
                )
            # Store results as csv file
            csv_file_name = os.path.join(self.local_path,
                                         f'{self.execution_date}')+'.csv'
            self.log.info(f'Type of result: {type(result)}')
            self.log.info(f'Write to file: {csv_file_name}')
            with open(csv_file_name,'w') as f:
                f.write(f'id,date_,data_value,m_flag,q_flag,s_flag,observ_time\n')
                f.write(f'{result}')

        #  # Check if file already exists and rename with timestamp-suffix
        #  full_filename = os.path.join(self.staging_location, self.s3_file)
        #  if os.path.isfile(full_filename):
        #      archive_filename = f'{full_filename}__{int(datetime.today().timestamp())}'
        #      self.log.info(f"""File '{full_filename}' already exists. Renaming to '{archive_filename}'""")
        #      os.rename(full_filename,archive_filename)
        #  tmp_filename = s3_hook.download_file(key=self.s3_key,
        #                        bucket_name=self.s3_bucket,
        #                        local_path=self.staging_location)
        #  # Rename downloaded file
        #  os.rename(os.path.join(self.staging_location, tmp_filename),
        #            full_filename)
        self.log.info(f'SelectFromNOAAS3ToStagingOperator successful.')

