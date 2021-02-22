import os
import subprocess
import boto3
from datetime import date, datetime
from airflow.models import Connection
from hooks.s3_hook_local import S3HookLocal
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class GetS3FileMetadata(BaseOperator):
    """ Downloads a file from Amazon S3 to
        the staging directory given by the
        data source type
    """

    ui_color = '#00FFFF'
    template_fields=["noaa_info",
                     "s3_bucket",
                     "s3_prefix",
                     "s3_keys"]

    @apply_defaults
    def __init__(self,
                 noaa_info: str = '',
                 s3_bucket: str ='',
                 s3_prefix: str ='',
                 s3_keys: list ='',
                 *args, **kwargs):

        super(GetS3FileMetadata, self).__init__(*args, **kwargs)
        self.noaa_info = noaa_info
        self.s3_bucket=s3_bucket
        self.s3_prefix=s3_prefix
        self.s3_keys=s3_keys

    def execute(self, context: dict) -> None:
        """ Use Boto3 to get metadata of certain S3 files
            Unfortunately, the metadata functionality required here 
            is not (yet) built into the aws hooks and operators.
        """

        aws_key = os.environ.get('AWS_KEY')
        aws_secret = os.environ.get('AWS_SECRET')
        aws_region = os.environ.get('AWS_REGION')
        self.log.info(f"Getting S3 metadata using boto3 library")
        s3_client = boto3.client('s3',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret,
                                region_name=aws_region)
        results = []
        for key in self.s3_keys:
            print(f"aws s3api head-object --bucket '{self.s3_bucket}' --key '{key}'")
            try:
                response = s3_client.head_object(Bucket=self.s3_bucket,
                                                 Key=key,
                                                 RequestPayer='requester',
                                                 ExpectedBucketOwner='')
            except:
                response=f'Could not execute  aws s3api head-object'
            else:
                results.append(response)
            print(f'------------------------- aws s3api head-object(): {response}')

