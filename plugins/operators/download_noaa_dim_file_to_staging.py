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
        #  self.aws_credentials=aws_credentials
        #  self.s3_bucket=s3_bucket
        #  self.s3_prefix=s3_prefix
        #  self.s3_key=s3_key
        #  self.local_path=local_path
        #  self.replace_existing=replace_existing
        #  print(f"""DownloadNOAADimFileToStagingOperator:
        #  {self.aws_credentials}
        #  {self.s3_bucket}
        #  {self.s3_prefix}
        #  {self.s3_key}
        #  {self.local_path}
        #  {self.replace_existing}
        #  """)




    #  def execute(self, context: dict) -> None:
    #      """ Check if *file* exists on NOAA S3 bucket Get the files LastModified
    #      date and compare with local staging area. Download the file from NOAA S3
    #      bucket if a newer version exists.  If LastModified date cannot be
    #      fetched from S3, no download is initiated.  """
    #
    #
    #      def get_last_modified_info_s3(s3_bucket: str,
    #                                    s3_prefix: str,
    #                                    s3_key: str) -> datetime:
    #          """ Use local environment variables to access noaa S3 bucket and
    #              obtain the last-modified info for *s3_key*
    #
    #              Returns: datetime.datetime of s3_key object's LastModified
    #                       property
    #                       datetime.datetime(1900,1,1, ....) if the object does not
    #                       exist or S3 access is not permitted.
    #          """
    #
    #          aws_key = os.environ.get('AWS_KEY','')
    #          aws_secret = os.environ.get('AWS_SECRET','')
    #          aws_region = os.environ.get('AWS_REGION','')
    #          print(f"AWS Environment variables: {aws_key}, {aws_secret}")
    #          # Make sure, AWS environment variables are set properly
    #          assert (aws_key != ''),    'AWS_KEY required in local environment'
    #          assert (aws_secret != ''), 'AWS_SECRET required in local environment'
    #          assert (aws_region != ''), 'AWS_REGION required in local environment'
    #          s3_client = boto3.client('s3',
    #                                   aws_access_key_id=aws_key,
    #                                   aws_secret_access_key=aws_secret,
    #                                   region_name=aws_region)
    #          try:
    #              response = s3_client.head_object(Bucket=s3_bucket,
    #                                               Key=os.path.join(s3_prefix, s3_key))
    #          except:
    #              self.log.info(f"Cannot get last-modified-date from S3 client\n"+
    #                            f"Using 1900-01-01 instead.")
    #              last_modified = datetime(1900, 1, 1, 0, 1, 1, tzinfo=tzutc()),
    #          else:
    #              last_modified = response['LastModified']
    #          return last_modified
    #
    #
    #      def get_last_modified_info_local(local_path: str,
    #                                       filename: str) -> datetime:
    #          """ Get last-modified info of *filename*
    #              If *local_path*/*filename* does not exists, return
    #              datetime.datetime(1900,1,1,...)
    #          """
    #          full_filename = os.path.join(local_path, filename)
    #          if os.path.exists(full_filename):
    #              last_modified_ts = os.path.getmtime(full_filename)
    #              last_modified = datetime.fromtimestamp(last_modified_ts,timezone.utc)
    #          else:
    #              last_modified = datetime(1900, 1, 1, 0, 1, 1, tzinfo=tzutc())
    #          return last_modified
    #
    #
    #      def download_file(s3_bucket: str, s3_prefix: str, s3_key: str,
    #                        local_path: str, replace_existing: str,
    #                        aws_credentials: any) -> None:
    #          """ Load single file from S3 to designated
    #              local staging location
    #
    #              s3_key: plain file name without prefix
    #          """
    #          full_s3_filename = os.path.join(s3_prefix, s3_key)
    #          full_local_filename = os.path.join(local_path, s3_key)
    #          self.log.info('Attempting to download'+
    #                        f's3://{s3_bucket}/{full_s3_filename}'+
    #                        f'to local staging: {full_local_filename}')
    #          # Check if local file already exists and
    #          # wether it should be overwritten or
    #          # moved to another file with timestamp-suffix added
    #          if os.path.isfile(full_local_filename):
    #              if replace_existing:
    #                  os.remove(full_local_filename)
    #                  self.log.info(f"Remove existing file '{full_local_filename}")
    #              else:
    #                  archive_filename = f'{full_local_filename}__{int(datetime.today().timestamp())}.bak'
    #                  self.log.info(f"File '{full_local_filename}' already exists."
    #                              + f" Renaming to '{archive_filename}'")
    #                  os.rename(full_local_filename, archive_filename)
    #
    #          # Make sure path for local staging exists
    #          if not os.path.exists(local_path):
    #              self.log.info(f"Creating path '{local_path}'")
    #              os.makedirs(local_path)
    #
    #          # Downloading the file
    #          s3_hook = S3Hook(aws_conn_id=aws_credentials)
    #          tmp_filename = s3_hook.download_file(key=full_s3_filename,
    #                                bucket_name=s3_bucket,
    #                                local_path=local_path)
    #          # Rename downloaded file if *tmp_filename* is a valid file
    #          if tmp_filename != "":
    #              os.rename(os.path.join(local_path, tmp_filename),
    #                        full_local_filename)
    #          self.log.info(f'... downloading s3://{s3_bucket}/{full_s3_filename} done.')
    #
    #
    #      # Check if s3_key exists on NOAA S3 and
    #      # obtain last-modified info at the same time
    #      last_modified_s3 = get_last_modified_info_s3(self.s3_bucket,
    #                                                   self.s3_prefix,
    #                                                   self.s3_key)
    #      # Get last-modified info for local staging file
    #      last_modified_local = get_last_modified_info_local(self.local_path, self.s3_key)
    #
    #      self.log.info(f"Last modification for '{self.s3_key}':"+
    #                    f"S3={last_modified_s3}, local={last_modified_local}")
    #      if last_modified_s3 > last_modified_local:
    #          self.log.info(f" ===> trying to download '{self.s3_key}' from S3.")
    #          download_file(self.s3_bucket, self.s3_prefix, self.s3_key,
    #                        self.local_path, self.replace_existing,
    #                        self.aws_credentials)
    #      else:
    #          self.log.info(f" ===> local '{self.s3_key}' is up to date.")
