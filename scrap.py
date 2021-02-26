import os
import boto3
from dateutil.tz import tzutc

aws_key = os.environ.get('AWS_KEY')
aws_secret = os.environ.get('AWS_SECRET')
aws_region = os.environ.get('AWS_REGION')

s3_client = boto3.client('s3',
                            aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret,
                            region_name=aws_region)

s3_client.get_bucket_acl(Bucket='noaa-ghcn-pds')

s3_client.list_objects(Bucket='noaa-ghcn-pds')

response = s3_client.head_object(Bucket='noaa-ghcn-pds', Key='status.txt')




{'ResponseMetadata': {
    'RequestId': 'F0C7A31AC0DEAC7D', 
    'HostId': 'RZEaVcCun5cBcpDrkP9FyP7sEMdpfrn2ff4Wl6Z+bFy5dOshAGfM5FIhfq3XRoXaaZVtM5t0I7E=', 
    'HTTPStatusCode': 200, 
    'HTTPHeaders': {
        'x-amz-id-2': 'RZEaVcCun5cBcpDrkP9FyP7sEMdpfrn2ff4Wl6Z+bFy5dOshAGfM5FIhfq3XRoXaaZVtM5t0I7E=', 
        'x-amz-request-id': 'F0C7A31AC0DEAC7D', 
        'date': 'Mon, 22 Feb 2021 11:28:50 GMT', 
        'last-modified': 'Tue, 02 Feb 2021 02:14:25 GMT', 
        'etag': '"11e3d6413aa0d5c27687f1d61b7c7cd9"', 
        'content-disposition': 'status.txt', 
        'accept-ranges': 'bytes', 
        'content-type': 'application/octet-stream', 
        'content-length': '34578', 
        'server': 'AmazonS3'}, 
    'RetryAttempts': 0}, 
'AcceptRanges': 'bytes', 
'LastModified': datetime.datetime(2021, 2, 2, 2, 14, 25, tzinfo=tzutc()),
'ContentLength': 34578, 
'ETag': '"11e3d6413aa0d5c27687f1d61b7c7cd9"', 
'ContentDisposition': 'status.txt', 
'ContentType': 'application/octet-stream', 
'Metadata': {}}



import os
from datetime import date, datetime
#from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from hooks.s3_hook_local import S3HookLocal
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operators.download_s3_file_to_staging import DownloadS3FileToStagingOperator
from helpers.sql_queries import SqlQueries

s3_hook = S3HookLocal(aws_conn_id='aws_credentials',local_dummy=False)

where_clause = "where s._2 >= '2021-02-20'"
f_sql = f"""select s._1 as id,
                   s._2 as date_,
                   s._3 as element,
                   s._4 as data_value,
                   s._5 as m_flag,
                   s._6 as q_flag,
                   s._7 as s_flag,
                   s._8 as observ_time
            from s3object s
            {where_clause}
            """

f_sql = "select * from s3object"

result = s3_hook.select_key(
    key = "csv.gz/2021.csv.gz",
    bucket_name = "noaa-ghcn-pds",
    expression  = f_sql,
    expression_type = "SQL",
    input_serialization = {
        'CSV': {
                   'FileHeaderInfo': 'NONE',
                   'FieldDelimiter': ','
                },
                'CompressionType': 'GZIP'
            },
    output_serialization = {
        'CSV': {
                'FieldDelimiter': f',',
                }
            }
    )



result = s3_hook.select_key(
    key = "csv/2020.csv",
    bucket_name = "noaa-ghcn-pds",
    expression  = "select s.* from s3object s",
    expression_type = "SQL",
    input_serialization = {
        'CSV': {}
        }
    )

result = s3_hook.read_key(
    key = "csv/2021.csv",
    bucket_name = "noaa-ghcn-pds",
    )
