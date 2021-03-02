import os
import gzip, glob
import json
from datetime import datetime
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class ReformatOpenAQJSONFilesOperator(BaseOperator):
    """
    Reformat a json file into csv so that it can be imported into
    the database via Copy_from. Depending on the parameters, the json file
    be plain text or gzipped.
    """

    ui_color = '#33BB99'
    template_fields=("local_path_raw",
                     "local_path_csv",)

    @apply_defaults
    def __init__(self,
                 local_path_raw: str = '',
                 local_path_csv: str = '',
                 delimiter:      str = '|',
                 quote:          str = '"',
                 add_header:     bool = True,
                 file_pattern: str = '*.ndjson.gz',
                 gzipped: bool = True,
                 *args, **kwargs):

        super(ReformatOpenAQJSONFilesOperator, self).__init__(*args, **kwargs)
        self.local_path_raw = local_path_raw
        self.local_path_csv = local_path_csv
        self.delimiter = delimiter
        self.quote = quote
        self.add_header = add_header
        self.file_pattern = file_pattern
        self.gzipped = gzipped


    def execute(self, context: dict) -> None:
        """
        Reformat json format to csv

        >> a nice way to parameterise the extraction of csv fields from
        >> json to a tabular format should be used here! 
        """

        def get_all_pattern_files(path: str, pattern: str) -> list:
            """ Return a list containing all <pattern> files
                from self.local_path
                <pattern> could be e.g. '*.csv'
            """
            all_files = glob.glob(os.path.join(path,pattern))
            return all_files

        def convert_json_line_to_str(json_line: str) -> str:
            """
            Extract OpenAQ-specific fields from json and
            return a csv-formatted string of these fields
            """
            json_data = json.loads(json_line)
            if json_data.get('date','key_error') != 'key_error':
                date_utc = str(json_data.get('date','').get('utc'))
                date_local = str(json_data.get('date','').get('local'))
            else:
                date_utc = ''
                date_local = ''
            parameter = str(json_data.get('parameter',''))
            value = str(json_data.get('value',''))
            unit = str(json_data.get('unit',''))
            location = str(json_data.get('location',''))
            city = str(json_data.get('city',''))
            country = str(json_data.get('country',''))
            if json_data.get('coordinates','key_error') != 'key_error':
                latitude = str(json_data.get('coordinates','').get('latitude'))
                longitude = str(json_data.get('coordinates','').get('longitude'))
            else:
                latitude = ''
                longitude = ''
            source_name = str(json_data.get('sourceName',''))
            source_type = str(json_data.get('sourceType',''))
            mobile = str(json_data.get('mobile',''))
            if json_data.get('averagingPeriod','key_error') != 'key_error':
                averaging_unit = str(json_data.get('averagingPeriod','').get('unit'))
                averaging_value = str(json_data.get('averagingPeriod','').get('value'))
            else:
                averaging_unit = ''
                averaging_value = ''
            f_output = [date_utc, date_local, parameter, value, unit,
                        location, city, country,
                        latitude, longitude,
                        source_name, source_type, mobile,
                        averaging_unit, averaging_value]
            f_str = self.delimiter.join(f_output)

            return f_str

        def reformat_json_data_into_csv(json_file: str,
                                        local_path_raw: str,
                                        local_path_csv: str) -> any:
            """
            Extract OpenAQ data fields from gzipped json file and
            write them into csv file
            """

            filename_only = os.path.relpath(json_file, local_path_raw)
            self.log.debug(f"json_file: {json_file}, local_path_raw: {local_path_raw}, local_path_csv: {local_path_csv}")

            filename_wo_suffix = os.path.splitext(os.path.splitext(filename_only)[0])[0]
            full_local_filename = os.path.join(local_path_csv, filename_wo_suffix)+'.csv'
            if not os.path.exists(self.local_path_csv):
                    os.makedirs(self.local_path_csv)

            self.log.debug(f"Writing json output to '{full_local_filename}'")
            with open(f'{full_local_filename}', 'w') as f_out:
                if self.add_header:
                    header = self.delimiter.join(['date_utc', 'date_local',
                                'parameter', 'value', 'unit',
                                'location', 'city', 'country',
                                'latitude', 'longitude',
                                'source_name', 'source_type', 'mobile',
                                'averaging_unit', 'averaging_value'])
                    f_out.write(f"{header}\n")
                if self.gzipped:
                    with gzip.open(os.path.join(self.local_path_raw,json_file),
                                   'rb') as f_in:
                        for line in f_in:
                            f_str = convert_json_line_to_str(line)
                            f_out.write(f"{f_str}\n")
                else:
                    with open(os.path.join(self.local_path_raw,json_file),
                              'r') as f_in:
                        for line in f_in:
                            f_str = convert_json_line_to_str(line)
                            f_out.write(f"{f_str}\n")

            return True


        self.log.debug(f"Run ReformatOpenAQJSONFilesOperator(,\n"+
                      f"    {self.local_path_raw},\n"+
                      f"    {self.local_path_csv},\n"+
                      f"    {self.delimiter},\n"+
                      f"    {self.quote},\n"+
                      f"    {self.add_header},\n"+
                      f"    {self.file_pattern},\n"+
                      f"    {self.gzipped})")

        all_json_files = get_all_pattern_files(self.local_path_raw,
                                               self.file_pattern)
        for filename in all_json_files:
            reformat_json_data_into_csv(filename,
                                        self.local_path_raw,
                                        self.local_path_csv)
