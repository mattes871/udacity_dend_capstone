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
