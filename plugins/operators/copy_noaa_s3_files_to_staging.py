import os
from datetime import date, datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CopyNOAAS3FilesToStagingOperator(BaseOperator):
    """ Copies a file from Amazon S3 to
        the staging directory given by the
        data source type
    """

    ui_color = '#FF0000'

    @apply_defaults
    def __init__(self,
                 aws_credentials='',
                 s3_bucket='',
                 s3_prefix='',
                 s3_files='',
                 staging_location='',
                 test_run=True,
                 *args, **kwargs):

        super(CopyNOAAS3FilesToStagingOperator, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.s3_bucket=s3_bucket
        self.s3_prefix=s3_prefix
        self.s3_files=s3_files
        self.staging_location=staging_location 
        self.test_run=test_run
        self.local_path=os.path.join(staging_location,
                                     date.today().strftime("%Y-%m-%d"))

    def execute(self, context):
        """ Copy a file from S3 to local staging directory
            Versioning is done using actual download date as suffix
        """

        def download_file(s3_file):
            """ Load single file from S3 to designated
                local staging location
            """
            full_s3_filename = os.path.join(self.s3_prefix, s3_file)
            full_local_filename = os.path.join(self.local_path,
                                               self.current_date_str,
                                               self.s3_prefix, s3_file)
            self.log.info('Attempting to download'+
                          f's3://{self.s3_bucket}/{full_s3_filename}'+
                          f'to local staging at {self.staging_location}')
            # Check if local file already exists and add timestamp-suffix to name
            if os.path.isfile(full_local_filename):
                archive_filename = f'{full_local_filename}__{int(datetime.today().timestamp())}'
                self.log.info(f"File '{full_local_filename}' already exists. Renaming to '{archive_filename}'")
                os.rename(full_local_filename,archive_filename)
            tmp_filename = s3_hook.download_file(key=full_s3_filename,
                                  bucket_name=self.s3_bucket,
                                  local_path=self.local_path)
            # Rename downloaded file
            os.rename(os.path.join(self.local_path, tmp_filename),
                      full_local_filename)
            self.log.info(f'... downloading s3://{self.s3_bucket}/{full_s3_filename} done.')


        self.log.info(f'Executing CopyNOAAS3FilesToStagingOperator ...')
        s3_hook = S3Hook(self.aws_credentials)
        self.log.info(f'Staging Location: {os.path.join(self.local_path,self.s3_prefix)} ------------------')
        # Make sure path for local staging exists
        if not os.path.exists(self.staging_location):
            os.makedirs(os.path.join(self.local_path,self.s3_prefix))

        for s3_file in self.s3_files:
            if not self.test_run:
                self.log.info(f'Call download_file({s3_file})')
                download_file(s3_file)
            else:
                self.log.info(f'TEST_RUN ... not executing download of: {s3_file}')

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
        self.log.info(f'CopyNOAAS3FilesToStagingOperator successful.')

