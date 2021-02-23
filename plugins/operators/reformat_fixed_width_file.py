import os
import boto3
from dateutil.tz import tzutc
from datetime import date, datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ReformatFixedWidthFileOperator(BaseOperator):
    """ 
    Open *local_path*/*filename* and transform the fixed-width format into a csv
    format with given delimiter. Add a header line for better readability in
    case of manual inspection of the staging area files.
    """

    ui_color = '"#3399FF'
    template_fields=[]

    @apply_defaults
    def __init__(self,
                 filename: str = '',
                 local_path_fixed_width: str = '',
                 local_path_csv: str = '',
                 column_names: list = ['all_in_one'],
                 column_positions: list = [0],
                 delimiter: str = '|',
                 add_header: bool = False,
                 remove_original_file: bool = True,
                 *args, **kwargs):

        super(ReformatFixedWidthFileOperator, self).__init__(*args, **kwargs)
        self.filename=filename
        self.local_path_fixed_width=local_path_fixed_width,
        self.local_path_csv=local_path_csv,
        self.column_names=column_names
        self.column_positions=column_positions,
        self.delimiter=delimiter
        self.add_header=add_header
        self.remove_original_file=remove_original_file
        


    def execute(self, context: dict) -> None:
        """ 
        Run the transformation from fixed-width to csv format
        """

        def reformat_file(filename: str, 
                          local_path_fixed_width: str,
                          local_path_csv: str,
                          column_positions: list,
                          delimiter: str,
                          remove_original_file: bool) -> None:
            """
            Open *filename*, transform line by line and write transformed lines
            back to a temporary file.
            Returns: filename: str with the name of the temporary file
            """
            full_fw_filename = os.path.join(local_path_fixed_width, filename)
            full_csv_filename = os.path.join(local_path_csv, filename)
            # if csv_file already exists, remove it
            try: os.remove(full_csv_filename)
            except: pass
            # if local_path_csv does not exist, create it
            if not os.path.exists(local_path_csv):
                self.log.info(f"Creating path '{local_path_csv}'")
                os.makedirs(local_path_csv)

            # Assuming that the local fw file exists 
            assert os.path.isfile(full_fw_filename), f"'{full_fw_filename}' does not exist"

            # Append max line lenght for splitting
            column_positions.append(255)
            with open(full_fw_filename, 'r') as f_fw:
                with open(full_csv_filename, 'w') as f_csv:
                    for line in f_fw:
                        splits = [line[column_positions[i]:column_positions[i+1]].strip() \
                                  for i in range(len(column_positions)-1)]
                        csv_line = delimiter.join(splits)
                        f_csv.write(f'{csv_line}\n')
            
            if remove_original_file:
                self.log.info(f"Removing original file: '{full_fw_filename}'")
                os.remove(full_fw_filename)
            else:
                self.log.info(f"Keeping original file: '{full_fw_filename}'")

        self.log.info(f"Executing ReformatFixedWidthFileOperator")
        reformat_file(self.filename, self.local_path_fixed_width[0],
                      self.local_path_csv[0], self.column_positions[0],
                      self.delimiter, self.remove_original_file)
