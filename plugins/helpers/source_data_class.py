from dataclasses import dataclass

@dataclass
class SourceDataClass:
    source_name: str       # short name of data source
    description: str
    source_type: str       # e.g. amazon s3
    source_params: dict    # e.g. bucket and key names, aws_credentials
    target_dir: str
    version: str


