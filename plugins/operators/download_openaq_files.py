import os
import boto3
from dateutil.tz import tzutc
from datetime import date, datetime, timezone
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.download_s3_file_to_staging import DownloadS3FileToStagingOperator

class DownloadOpenAQFilesOperator(BaseOperator):
    """
    Specialisation of the DownloadS3File Operator.
    In case functionality specific to the NOAA dimension files need to be
    implemented, here is the place to do so.
    All common functionality for downloading files from S3 and storing them on a
    local staging folder is implemented in the DownloadS3FileToStaging operator

    If a new version of the specified file exists in the NOAA S3 bucket then
    download it to the local staging area. Otherwise do nothing.

    If the *head_object* operation for S3 cannot be performed, it is
    assumed that the most recent file is already on local staging, i.e. no
    download is triggered to avoid unnecessary transfer costs
    """

    ui_color = '"#33FFFF'
    template_fields=["s3_bucket",
                     "s3_prefix"]

    @apply_defaults
    def __init__(self,
                 aws_credentials: str = '',
                 s3_bucket:  str = '',
                 s3_prefix:  str = '',
                 local_path: str = '',
                 *args, **kwargs):

        super(DownloadOpenAQFilesOperator, self).__init__(
            *args, **kwargs)
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.local_path = local_path


    def execute(self, context: dict):
        self.log.info(f"Executing DownloadOpenAQFilesOperator")
        self.log.info(f"s3_prefix: {self.s3_prefix}")
        # Get s3_hook
        s3_hook = S3Hook(aws_conn_id = self.aws_credentials)
        # Make sure, the local path exists
        if not os.path.exists(self.local_path):
            self.log.info(f"Creating path '{self.local_path}'")
            os.makedirs(self.local_path)
        # Get all files from folder
        all_files = s3_hook.list_keys(bucket_name=self.s3_bucket,
                                      prefix=self.s3_prefix,
                                      delimiter='/',
                                      max_items=99999)
        self.log.info(f"Found {len(all_files)} files. First 20: {all_files[:20]}")
        self.log.info(f"Start downloading ")
        for file in all_files:
            full_s3_filename = os.path.join(self.s3_prefix, file)
            full_local_filename = os.path.join(self.local_path, file)
            # Download only if file does not exists locally
            if not os.isfile(full_local_filename):
                # Downloading the file
                tmp_filename = s3_hook.download_file(key=full_s3_filename,
                                      bucket_name=self.s3_bucket,
                                      local_path=self.local_path)
                # Rename downloaded file if *tmp_filename* is a valid file
                if tmp_filename != "":
                    os.rename(os.path.join(self.local_path, tmp_filename),
                              full_local_filename)
        self.log.info(f'... downloading done.')
