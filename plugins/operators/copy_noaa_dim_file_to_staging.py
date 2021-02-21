import os
import subprocess
import boto3
from datetime import date, datetime
from hooks.s3_hook_local import S3HookLocal
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CopyNOAADimFileToStaging(BaseOperator):
    """ Download the specified file from the NOAA S3 bucket
        if a newer version of this file exists on S3 as 
        compared to the local staging area
    """

    ui_color = '#AAAA00'
    template_fields=["s3_bucket",
                     "s3_prefix",
                     "s3_keys"]

    @apply_defaults
    def __init__(self,
                 aws_credentials: str ='',
                 s3_bucket: str ='',
                 s3_prefix: str ='',
                 s3_key:    str ='',
                 *args, **kwargs):

        super(GetS3FileMetadata, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.s3_bucket=s3_bucket
        self.s3_prefix=s3_prefix
        self.s3_key=s3_key

    def execute(self, context: dict) -> None:
        """ Check if *file* exists on NOAA S3 bucket
            Get the files LastModified date and compare
            with local staging area. Download the file
            from NOAA S3 bucket if a newer version exists.
        """

        # Check if s3_key exists on NOAA S3
        try:
           pass
        except:
            pass
        s3_client = boto3.client('s3',
                                aws_access_key_id='',
                                aws_secret_access_key='',
                                region_name='eu-central-1')
        results = []
        for key in self.s3_keys:
            print(f"YYYY: {self.s3_bucket} / {key}")
            try:
                response = s3_client.head_object(Bucket=self.s3_bucket,
                                                 Key=key,
                                                 RequestPayer='requester')
            except:
                response=f'Could not execute  aws s3api head-object'
            else:
                results.append(response)
            print(f'------------------------- aws s3api head-object(): {response}')

