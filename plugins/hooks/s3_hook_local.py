import os
from datetime import date, datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3HookLocal(S3Hook):
    """ Acts as a normal S3Hook or delivers local dummy data
        depending whether the parameter *local_dummy* is set
        to True or False. This is mainly to avoid costs while
        testing other parts of the DAG
    """

    def __init__(self,
                 local_dummy=True,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.local_dummy=local_dummy

    def select_key(self, key: str, 
                   bucket_name: str = None,
                   expression: str = None, 
                   expression_type: str = None,
                   input_serialization: dict = None, 
                   output_serialization: dict = None, 
                   *args, **kwargs):
        if self.dummy_local:
            self.log.info(f'Passing S3HookLocal.select_key')
            pass
        else:
            super() \
                .select_key(key, bucket_name, expression,
                            expression_type, input_serialization,
                            output_serialization, *args, **kwargs)

    def download_file(self, key: str, bucket_name: str = None,
                      local_path: str = None, 
                      *args, **kwargs):
        if self.dummy_local:
            self.log.info(f'Passing S3HookLocal.download_file')
            pass
        else:
            super().download_files(key, bucket_name, local_path,
                                   *args, **kwargs)
