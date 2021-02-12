import os
from datetime import date, datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SelectFromNOAAS3ToStagingOperator(BaseOperator):
    """ Select records for a specific day from the
        NOAA table file on Amazon S3.
        
        and write them onto the staging location as 
        csv file.
    """

    ui_color = '#00FF00'
    template_fields=["execution_date_str",
                     "s3_table_file",
                     "local_path"]

    @apply_defaults
    def __init__(self,
                 aws_credentials='',
                 s3_bucket='',
                 s3_prefix='',
                 s3_table_file='',
                 most_recent_data_date_str='19000101',
                 execution_date_str='',
                 real_date_str='',
                 local_path='',
                 test_run=True,
                 *args, **kwargs):

        super(SelectFromNOAAS3ToStagingOperator, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.s3_bucket=s3_bucket
        self.s3_prefix=s3_prefix
        self.s3_table_file=s3_table_file
        self.most_recent_data_date_str=most_recent_data_date_str
        self.execution_date_str=execution_date_str
        self.real_date_str=real_date_str
        self.local_path=local_path  # +"{{ execution_date.year }}",
        self.test_run=test_run

    def execute(self, context):
        """ Select all records for the **execution_date** day from the
            NOAA s3_table_file on Amazon S3 and store the records
            as a csv file on the local staging area under
            <local_path>/<real_date>
            If more than one csv file is created, all of them will be
            in the same directory  just different file name
        """

        self.log.info(f'Executing SelectFromNOAAS3ToStagingOperator ...')
        self.log.info(f'Params:\nMost recent: {self.most_recent_data_date_str},'+
                      f'\nExecution date: {self.execution_date_str},'+
                      f'\nBucket & Table File: {self.s3_bucket} + {self.s3_table_file}'+
                      f'\nStaging Location: {self.local_path}')
        s3_hook = S3Hook(self.aws_credentials)
        # Make sure path for local staging exists
        most_recent_year=self.most_recent_data_date_str[0:3]
        most_recent_month=self.most_recent_data_date_str[4:5]
        execution_year=self.execution_date_str[0:3]
        execution_month=self.execution_date_str[4:5]
        execution_day=self.execution_date_str[6:7]
        if not os.path.exists(self.local_path):
            self.log.info(f'Create Staging Location Folder')
            os.makedirs(self.local_path)

        # Restrict to records that are newer than the 'most_recent'
        # record in the staging table
        #
        # >>>> WIP: What if 'most_recent' is in past year
        # >>>>      records need to be loaded from different file
        #
        where_clause = f"where s._2 > '{self.most_recent_data_date_str}'"
        self.log.info(f"""select s._1 as id,
                                  s._2 as date_,
                                  s._3 as data_value,
                                  s._4 as m_flag,
                                  s._5 as q_flag,
                                  s._6 as s_flag,
                                  s._7 as observ_time
                           from s3object s
                           {where_clause}
                           limit 32""")
        result = s3_hook.select_key(
            key=f'{self.s3_prefix}/{self.s3_table_file}',
            bucket_name=f'{self.s3_bucket}',
            expression=f"""select s._1 as id,
                                  s._2 as date_,
                                  s._3 as data_value,
                                  s._4 as m_flag,
                                  s._5 as q_flag,
                                  s._6 as s_flag,
                                  s._7 as observ_time
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
        csv_file_name = os.path.join(self.local_path,self.execution_date_str)+'.csv'
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

