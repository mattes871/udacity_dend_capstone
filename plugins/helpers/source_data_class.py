from dataclasses import dataclass
from datetime import datetime

@dataclass
class SourceDataClass:
    source_name: str       # short name of data source
    description: str
    source_type: str       # e.g. amazon s3
    source_params: dict    # e.g. bucket and key names, aws_credentials
    data_available_from: datetime    # first year with data for this source
    staging_location: str
    version: str

