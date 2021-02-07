import os
from datetime import date, datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SelectFromS3ToStagingOperator(BaseOperator):
    """ Select records for a specific day from the
        NOAA table file on Amazon S3.
        
        and write them onto the staging location as 
        csv file.
    """

    ui_color = '#00FF00'

    @apply_defaults
    def __init__(self,
                 aws_credentials='',
                 s3_bucket='',
                 s3_prefix='',
                 s3_table_file='',
                 execution_date_str='',
                 real_date_str='',
                 staging_location='',
                 *args, **kwargs):

        super(SelectFromS3ToStagingOperator, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.s3_bucket=s3_bucket
        self.s3_prefix=s3_prefix
        self.s3_table_file=s3_table_file
        self.execution_date_str=execution_date_str
        self.real_date_str=real_date_str
        self.staging_location=os.path.join(staging_location,
                                           execution_date_str[0:3],
                                           execution_date_str[4:5])

    def execute(self, context):
        """ Select all records for the **execution_date** day from the 
            NOAA s3_table_file on Amazon S3 and store the records
            as a csv file on the local staging area under
            <staging_location>/<exec_year>/<exec_month>/<date_date>_<real_date>
        """

        #  def rename_file():
        #      """ Load single file from S3 to designated
        #          local staging location
        #      """
        #      full_s3_filename = os.path.join(self.s3_prefix, s3_file)
        #      full_local_filename = os.path.join(self.staging_location,
        #                                         self.s3_prefix, s3_file)
        #      self.log.info('Attempting to download'+
        #                    f's3://{self.s3_bucket}/{full_s3_filename}'+
        #                    f'to local staging at {self.staging_location}')
        #      # Check if local file already exists and add timestamp-suffix to name
        #      if os.path.isfile(full_local_filename):
        #          archive_filename = f'{full_local_filename}__{int(datetime.today().timestamp())}'
        #          self.log.info(f"File '{full_local_filename}' already exists. Renaming to '{archive_filename}'")
        #          os.rename(full_local_filename,archive_filename)
        #      tmp_filename = s3_hook.download_file(key=full_s3_filename,
        #                            bucket_name=self.s3_bucket,
        #                            local_path=self.staging_location)
        #      # Rename downloaded file
        #      os.rename(os.path.join(self.staging_location, tmp_filename),
        #                full_local_filename)
        #      self.log.info(f'... downloading s3://{self.s3_bucket}/{full_s3_filename} done.')

        self.log.info(f'Executing SelectFromS3ToStagingOperator ...')
        s3_hook = S3Hook(self.aws_credentials)
        # Make sure path for local staging exists
        exec_year=self.execution_date_str[0:3]
        exec_month=self.execution_date_str[4:5]
        exec_day=self.execution_date_str[6:7]
        os.makedirs(os.path.join(self.staging_location))
        self.log.info(f"""Execution date: {self.execution_date_str}
                          real date: {self.real_date_str}""")
        result = s3_hook.select_key(
              key='{self.s3_prefix}/{execution_year}.csv.gz',
              bucket_name=self.s3_bucket,
              expression=f"""select s._1 as id,
                                    s._2 as date_,
                                    s._3 as data_value,
                                    s._4 as m_flag,
                                    s._5 as q_flag,
                                    s._6 as s_flag,
                                    s._7 as observ_time
                             from s3object s
                             where s._2='{execution_year}{execution_month}{execution_day}' 
                             limit 255""",
              input_serialization={
                  'CSV': {
                         'FileHeaderInfo': 'NONE',
                         'FieldDelimiter': ','
                      },
                  'CompressionType': 'GZIP'
                  }
              )
        # Store results as csv file
        self.log.info(f'Type of result: {type(result)}')

        with open(f'{self.staging_location}/{self.execution_date_str}.txt') as f:
            write(result)

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
        self.log.info(f'SelectFromS3ToStagingOperator successful.')

