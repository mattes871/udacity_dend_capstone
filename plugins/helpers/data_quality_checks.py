from airflow.models import Variable

class DataQualityChecks:
    """
    Collection of Data Quality checks for dimension and facts table.
    Quality checks are done on staging before data is transfered to the
    production tables.
    """
    general_config: dict = Variable.get("general", deserialize_json=True)
    CSV_QUOTE_CHAR = general_config['csv_quote_char']
    CSV_DELIMITER = general_config['csv_delimiter']
    NOAA_STAGING_SCHEMA = general_config['noaa_staging_schema']
    PRODUCTION_SCHEMA = general_config['production_schema']

    dq_checks_noaa_dim=[
    {'sql': f'SELECT COUNT(*) FROM {NOAA_STAGING_SCHEMA}.ghcnd_stations_raw WHERE id is null', 'expected': True, 'value': 0},
    {'sql': f'SELECT COUNT(*) FROM {NOAA_STAGING_SCHEMA}.ghcnd_countries_raw WHERE country_id is null', 'expected': True, 'value': 0},
    {'sql': f'SELECT COUNT(*) FROM {NOAA_STAGING_SCHEMA}.ghcnd_inventory_raw WHERE id is null', 'expected': True, 'value': 0}
    ]

    dq_checks_noaa_facts=[
    {'sql': f'SELECT COUNT(*) FROM {NOAA_STAGING_SCHEMA}.f_weather_data_raw WHERE id is null', 'expected': True, 'value': 0}
    ]

    dq_checks_openaq=[
    {'sql': f'SELECT COUNT(*) FROM {OPENAQ_STAGING_SCHEMA}.f_air_data_raw WHERE location is null', 'expected': True, 'value': 0}
    ]
