from airflow.models import Variable

class SqlQueries:
    """
    Collection of all SQL queries to be used in the pipeline.
    """

    general_config: dict = Variable.get("general", deserialize_json=True)
    CSV_QUOTE_CHAR = general_config['csv_quote_char']
    CSV_DELIMITER = general_config['csv_delimiter']
    NOAA_STAGING_SCHEMA = general_config['noaa_staging_schema']
    PRODUCTION_SCHEMA = general_config['production_schema']


    noaa_most_recent_data = (f"""
        SELECT to_char(max(date_),'YYYY-MM-DD') as max_date_str
              FROM {PRODUCTION_SCHEMA}.f_climate_data
              WHERE source = 'noaa' ;
        """)

    # Use only the 5 most relevant KPIs from NOAA, 65 additional KPIs can be found
    # in that source.
    noaa_populate_d_kpi_table = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.d_kpi_names (
            orig_kpi_name, common_kpi_name, source, description)
        VALUES
           ('PRCP', 'PRCP', 'noaa', 'Precipitation (tenths of mm)'),
           ('SNOW', 'SNOW', 'noaa', 'Snowfall (mm)'),
           ('SNWD', 'SNWD', 'noaa', 'Snow depth (mm)'),
           ('TMAX', 'TMAX', 'noaa', 'Maximum temperature (tenths of degrees C)'),
           ('TMIN', 'TMIN', 'noaa', 'Minimum temperature (tenths of degrees C)')
        ON CONFLICT (orig_kpi_name, source) DO NOTHING ;
        """)

    # Use only the 5 most relevant KPIs from OpenAQ
    openaq_populate_d_kpi_table = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.d_kpi_names (
            orig_kpi_name, common_kpi_name, source, description)
        VALUES
           ('o3', 'O3', 'opaq', 'Ozone'),
           ('pm25', 'PM25', 'opaq', '???'),
           ('pm10', 'PM10', 'opaq', '???'),
           ('co', 'CO', 'opaq', 'Carbonmonoxide'),
           ('no2', 'NO2', 'opaq', 'Nitrogene dioxide')
        ON CONFLICT (orig_kpi_name, source) DO NOTHING ;
        """)

    load_noaa_stations = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.d_stations
        SELECT
            'noaa' || id as station_id,
            'noaa' as source,
            latitude::numeric,
            longitude::numeric,
            elevation::int,
            state, name
        FROM {NOAA_STAGING_SCHEMA}.ghcnd_stations_raw
        WHERE id is not null
        ON CONFLICT (station_id) DO NOTHING
        ;
        """)

    # The inner join with *d_kpi_names* returns only records for KPIs that
    # are mapped in *d_kpi_names*
    load_noaa_inventory = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.d_inventory
        SELECT
            'noaa' || raw.id as station_id,
            'noaa' as source,
            kpi.common_kpi_name,
            raw.from_year as from_year,
            raw.until_year as until_year
        FROM {NOAA_STAGING_SCHEMA}.ghcnd_inventory_raw as raw
        JOIN {PRODUCTION_SCHEMA}.d_kpi_names as kpi
        ON raw.element = kpi.orig_kpi_name
        ON CONFLICT (station_id, common_kpi_name) DO UPDATE
            SET (from_year, until_year) = (EXCLUDED.from_year,
                                           EXCLUDED.until_year)
        ;
        """)


    load_noaa_countries = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.d_countries
        SELECT
            country_id,
            country,
            'noaa' as source
        FROM {NOAA_STAGING_SCHEMA}.ghcnd_countries_raw
        ON CONFLICT (country_id, source) DO NOTHING
        ;
        """)

    # The inner join with *d_kpi_names* returns only records for KPIs that
    # are mapped in *d_kpi_names*
    transform_noaa_facts = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.f_climate_data
        SELECT
            'noaa' || id as station_id,
            'noaa' as source,
            to_date(raw.date_,'YYYYMMDD') as date_,
            kpi.common_kpi_name,
            raw.data_value::int as data_value,
            raw.observ_time as observ_time
        FROM {NOAA_STAGING_SCHEMA}.f_weather_data_raw as raw
        JOIN {PRODUCTION_SCHEMA}.d_kpi_names as kpi
        ON raw.element = kpi.orig_kpi_name
        ON CONFLICT (station_id, date_, common_kpi_name) DO NOTHING
        ;
        """)

    aggregate_ger_monthly_data = (f"""
        CREATE TABLE IF NOT EXISTS {PRODUCTION_SCHEMA}.ol_mthly_analytic_ger AS
        SELECT
            f_agg.station_id,
            month,
            max(CASE WHEN common_kpi_name = 'TMAX' THEN max_data_value ELSE -9999 END)/10.0 as max_tmax_c,
            max(CASE WHEN common_kpi_name = 'TMIN' THEN min_data_value ELSE -9999 END)/10.0  as min_tmin_c,
            max(CASE WHEN common_kpi_name = 'TMAX' THEN avg_data_value ELSE -9999 END)/10.0  as avg_tmax_c,
            max(CASE WHEN common_kpi_name = 'TMIN' THEN avg_data_value ELSE -9999 END)/10.0  as avg_tmin_c,
            sum(CASE WHEN common_kpi_name = 'PRCP' THEN sum_data_value ELSE 0 END) as sum_prcp
        FROM
            (SELECT
                station_id,
                common_kpi_name,
                date_trunc('month',date_) as month,
                avg(data_value) as avg_data_value,
                min(data_value) as min_data_value,
                max(data_value) as max_data_value,
                sum(data_value) as sum_data_value
            FROM {PRODUCTION_SCHEMA}.f_climate_data
            WHERE substr(station_id,5,2) = 'GM' -- Germany
            GROUP BY station_id, month, common_kpi_name) as f_agg
        JOIN {PRODUCTION_SCHEMA}.d_stations as d
        ON f_agg.station_id = d.station_id
        GROUP BY f_agg.station_id, month
        ;
        """)

    def create_partition_table_cmd(schema: str,
                                   table: str,
                                   year: int) -> str:
        """
        Given the year, generate the SQL command for creating a partition table
        """
        f_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table}_{year} PARTITION OF {schema}.{table}
            FOR VALUES FROM ('{year}-01-01') TO ('{year+1}-01-01') ;
            """
        return f_sql
