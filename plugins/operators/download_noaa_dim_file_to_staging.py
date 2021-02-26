import os
import boto3
from dateutil.tz import tzutc
from datetime import date, datetime, timezone
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.download_s3_file_to_staging import DownloadS3FileToStagingOperator



class DownloadNOAADimFileToStagingOperator(DownloadS3FileToStagingOperator):
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
                     "s3_prefix",
                     "s3_key"]

    @apply_defaults
    def __init__(self,
                 aws_credentials: str = '',
                 s3_bucket:  str = '',
                 s3_prefix:  str = '',
                 s3_key:     str = '',
                 local_path: str = '',
                 replace_existing: bool = True,
                 *args, **kwargs):

        super(DownloadNOAADimFileToStagingOperator, self).__init__(
            aws_credentials=aws_credentials,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            s3_key=s3_key,
            local_path=local_path,
            replace_existing=replace_existing,
            *args, **kwargs)

