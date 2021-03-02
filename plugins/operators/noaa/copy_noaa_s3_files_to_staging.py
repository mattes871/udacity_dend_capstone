import os
from datetime import date, datetime
#from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from hooks.s3_hook_local import S3HookLocal
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CopyNOAAS3FilesToStagingOperator(BaseOperator):
    """ Downloads a file from Amazon S3 to
        the staging directory given by the
        data source type
    """

    ui_color = '#AAAACC'

    @apply_defaults
    def __init__(self,
                 aws_credentials: str ='',
                 s3_bucket: str ='',
                 s3_prefix: str ='',
                 s3_keys: str ='',
                 local_path: str ='',
                 replace_existing: bool =True,
                 *args, **kwargs):

        super(CopyNOAAS3FilesToStagingOperator, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.s3_bucket=s3_bucket
        self.s3_prefix=s3_prefix
        self.s3_keys=s3_keys
        self.local_path=local_path
        self.replace_existing=replace_existing,

    def execute(self, context: dict) -> None:
        """ Copy a file from S3 to local staging directory
            Versioning is done using actual download date as suffix
        """

        def download_file(s3_key: str) -> None:
            """ Load single file from S3 to designated
                local staging location

                s3_key: plain file name without prefix
            """
            full_s3_filename = os.path.join(self.s3_prefix, s3_key)
            full_local_filename = os.path.join(self.local_path, s3_key)
            self.log.info('Attempting to download'+
                          f's3://{self.s3_bucket}/{full_s3_filename}'+
                          f'to local staging at {self.local_path}')
            # Check if local file already exists and
            # wether it should be overwritten or
            # moved to another file with timestamp-suffix added
            if os.path.isfile(full_local_filename):
                if self.replace_existing:
                    os.remove(full_local_filename)
                    self.log.info(f"Remove existing file '{full_local_filename}")
                else:
                    archive_filename = f'{full_local_filename}__{int(datetime.today().timestamp())}'
                    self.log.info(f"File '{full_local_filename}' already exists."
                                + f" Renaming to '{archive_filename}'")
                    os.rename(full_local_filename,archive_filename)
            # Downloading the file
            tmp_filename = s3_hook.download_file(key=full_s3_filename,
                                  bucket_name=self.s3_bucket,
                                  local_path=self.local_path)
            # Rename downloaded file
            os.rename(os.path.join(self.local_path, tmp_filename),
                      full_local_filename)
            self.log.info(f'... downloading s3://{self.s3_bucket}/{full_s3_filename} done.')


        self.log.info(f'Executing CopyNOAAS3FilesToStagingOperator ...')
        s3_hook = S3Hook(self.aws_credentials)
        self.log.info(f'Staging Location: {self.local_path}')
        # Make sure path for local staging exists
        if not os.path.exists(self.local_path):
            os.makedirs(self.local_path)

        # Download all files in list
        for s3_key in self.s3_keys:
            self.log.info(f'Download_file({s3_key})')
            download_file(s3_key)

        self.log.info(f'CopyNOAAS3FilesToStagingOperator successful.')
