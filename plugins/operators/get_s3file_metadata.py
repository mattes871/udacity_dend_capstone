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

    ui_color = '#AAAAFF'
    template_fields=["s3_bucket",
                     "s3_prefix",
                     "s3_keys"]

    @apply_defaults
    def __init__(self,
                 aws_credentials: str ='',
                 s3_bucket: str ='',
                 s3_prefix: str ='',
                 s3_keys: list ='',
                 *args, **kwargs):

        super(GetS3FileMetadata, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.s3_bucket=s3_bucket
        self.s3_prefix=s3_prefix
        self.s3_keys=s3_keys

    def execute(self, context: dict) -> None:
        """ Use AWS CLI to get metadata of certain S3 files
            Unfortunately, metadata functionality is not (yet)
            built into the aws hooks and operators
            AWSCLI must be installed
        """

        #aws_cred = Connection.get_password(f'{self.aws_credentials}')
        #print(f"AWS Credentials: {aws_cred}")
        #s3_hook = S3Hook(self.aws_credentials)
        #s3_conn = s3_hook.get_conn()
        #print(f"AWS Conn: {s3_conn}")
        s3_pwd = Connection('aws_credentials').get_uri()
        print(f"AWS PWD: {s3_pwd}")        
        print(f"Credentials: {os.environ.get('AWS_KEY')}, {os.environ.get('AWS_SECRET')}")
        s3_client = boto3.client('s3',
                                aws_access_key_id=os.environ.get('AWS_KEY'),
                                aws_secret_access_key=os.environ.get('AWS_SECRET_URI'),
                                region_name=os.environ.get('AWS_REGION'))
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

