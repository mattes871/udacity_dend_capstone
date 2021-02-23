import os
from airflow import DAG
from datetime import datetime
from operators.download_noaa_dim_file_to_staging import DownloadNOAADimFileToStagingOperator

#from airflow.operators.dummy import DummyOperator
#from airflow.utils.decorators import apply_defaults

#  class S3DummyOperator(DummyOperator):
#
#      @apply_defaults
#      def __init__(self,
#                   aws_credentials: str,
#                   s3_bucket: str,
#                   s3_prefix: str,
#                   s3_key: str,
#                   replace_existing: bool,
#                   local_path: str,
#                   *args, **kwargs) -> DummyOperator:
#
#          super(S3DummyOperator, self).__init__(*args, **kwargs)
#          self.s3_key=s3_key
#          self.s3_bucket=s3_bucket
#
#      def execute(self, context: dict) -> None:
#          print(f'S3: {self.s3_bucket}/{self.s3_key}   <<<<<<<<<<<<<<<<<<')
#          return super(S3DummyOperator, self).execute(context)


def download_noaa_dim_subdag(parent_dag_name: str, task_id: str, 
                             aws_credentials: str = '',
                             s3_bucket: str = '',
                             s3_prefix: str = '',
                             s3_keys: list  = [], 
                             replace_existing: bool = True,
                             local_path: str = '',
                             *args, **kwargs) -> DAG:

    """
    Generate a DAG to be used as a subdag.

    :param str parent_dag_name: Id of the parent DAG
    :param str child_dag_name: Id of the child DAG
    :param dict args: Default arguments to provide to the subdag
    :return: DAG to use as a subdag
    :rtype: airflow.models.DAG
    """

    dag_download_noaa_dim_subdag = DAG(
        dag_id=f'{parent_dag_name}.{task_id}',
        start_date=datetime.today(),
        schedule_interval='@daily',
    )

    for s3_key in s3_keys:
       download_s3_file = DownloadNOAADimFileToStagingOperator(
            task_id=f'{task_id}.{s3_key[:-4]}',
            aws_credentials=aws_credentials,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            s3_key=s3_key,
            replace_existing=replace_existing,
            local_path=local_path,
            dag=dag_download_noaa_dim_subdag,
            *args, **kwargs
            )
       download_s3_file

    return dag_download_noaa_dim_subdag

