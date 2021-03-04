from airflow.models import Variable

class SqlQueries:
    """
    Collection of all SQL queries to be used in the pipeline.
    """

    general_config: dict = Variable.get("general", deserialize_json=True)
    CSV_QUOTE_CHAR = general_config['csv_quote_char']
    CSV_DELIMITER = general_config['csv_delimiter']
    NOAA_STAGING_SCHEMA = general_config['noaa_staging_schema']
    OPENAQ_STAGING_SCHEMA = general_config['openaq_staging_schema']
    PRODUCTION_SCHEMA = general_config['production_schema']

    noaa_config: dict = Variable.get("noaa_config", deserialize_json=True)
    NOAA_DATA_AVAILABLE_FROM: str = noaa_config['data_available_from']

    openaq_config: dict = Variable.get("openaq_config", deserialize_json=True)
    OPENAQ_DATA_AVAILABLE_FROM: str = openaq_config['data_available_from']

    #
    # ************** NOAA *************
    #

    noaa_most_recent_data = (f"""
        SELECT to_char(max(date_),'YYYY-MM-DD') as max_date_str
              FROM {PRODUCTION_SCHEMA}.f_climate_data
              WHERE source = 'noaa' ;
        """)

    # Use only the 5 most relevant KPIs from NOAA, 65 additional KPIs can be found
    # in that source.
    noaa_populate_d_kpi_table = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.d_kpi_names (
            orig_kpi_name, orig_unit, common_kpi_name, common_unit, source, description)
        VALUES
           ('PRCP', '1/10mm', 'PRCP', 'mm', 'noaa', 'Precipitation (tenths of mm)'),
           ('SNOW', 'mm', 'SNOW', 'mm', 'noaa', 'Snowfall (mm)'),
           ('SNWD', 'mm', 'SNWD', 'mm', 'noaa', 'Snow depth (mm)'),
           ('TMAX', '1/10°C', 'TMAX', '°C', 'noaa', 'Maximum temperature (tenths of degrees C)'),
           ('TMIN', '1/10°C', 'TMIN', '°C', 'noaa', 'Minimum temperature (tenths of degrees C)')
        ON CONFLICT (orig_kpi_name, orig_unit, source) DO NOTHING ;
        """)

    load_noaa_stations = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.d_stations
        SELECT
            id as station_id,
            'noaa' as source,
            latitude::numeric,
            longitude::numeric,
            elevation::numeric,
            left(id,2) as country_id,
            name
        FROM {NOAA_STAGING_SCHEMA}.ghcnd_stations_raw
        WHERE id is not null
        ON CONFLICT (station_id, source) DO NOTHING
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
            kpi.common_unit,
            raw.from_year as from_year,
            raw.until_year as until_year
        FROM {NOAA_STAGING_SCHEMA}.ghcnd_inventory_raw as raw
        JOIN {PRODUCTION_SCHEMA}.d_kpi_names as kpi
        ON raw.element = kpi.orig_kpi_name
           -- AND raw.unit=kpi.orig_unit / not necessary for noaa source
        ON CONFLICT (station_id, common_kpi_name, common_unit) DO UPDATE
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
        INSERT INTO {PRODUCTION_SCHEMA}.f_climate_data (
            station_id, source, date_,
            common_kpi_name, common_unit, data_value,
            observ_time
        )
        SELECT
            id as station_id,
            'noaa' as source,
            to_date(raw.date_,'YYYYMMDD') as date_,
            kpi.common_kpi_name,
            kpi.common_unit,
            CASE WHEN kpi.common_kpi_name IN ('TMAX','TMIN','PRCP')
                THEN (raw.data_value::numeric) / 10.0
                ELSE raw.data_value::numeric
            END as data_value,
            raw.observ_time as observ_time
        FROM {NOAA_STAGING_SCHEMA}.f_weather_data_raw as raw
        JOIN {PRODUCTION_SCHEMA}.d_kpi_names as kpi
        ON raw.element = kpi.orig_kpi_name
        ON CONFLICT (station_id, date_, common_kpi_name, common_unit) DO NOTHING
        ;
        """)

    delete_noaa_staging_data = (f"""
        BEGIN ;
        TRUNCATE TABLE {NOAA_STAGING_SCHEMA}.f_weather_data_raw ;
        TRUNCATE TABLE {NOAA_STAGING_SCHEMA}.ghcnd_stations_raw ;
        TRUNCATE TABLE {NOAA_STAGING_SCHEMA}.ghcnd_inventory_raw ;
        TRUNCATE TABLE {NOAA_STAGING_SCHEMA}.ghcnd_countries_raw ;
        END ;
        """)


    #
    # ************** OpenAQ *************
    #

    # Use only the 5 most relevant KPIs from OpenAQ
    openaq_populate_d_kpi_table = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.d_kpi_names (
            orig_kpi_name, orig_unit, common_kpi_name, common_unit, source, description)
        VALUES
           ('o3', 'ppm', 'O3', 'ppm', 'openaq', 'Ozone in ppm'),
           ('co', 'ppm', 'CO', 'ppm', 'openaq', 'Carbon monoxide in ppm'),
           ('no2', 'ppm', 'NO2', 'ppm', 'openaq', 'Nitrogene dioxide in ppm'),
           ('so2', 'ppm', 'SO2', 'ppm', 'openaq', 'Sulfur dioxide in ppm'),
           ('o3', 'µg/m³', 'O3', 'µg/m³', 'openaq', 'Ozone in µg/m³'),
           ('pm25', 'µg/m³', 'PM25', 'µg/m³', 'openaq', 'Particle matter smaller 2.5µm'),
           ('pm10', 'µg/m³', 'PM10', 'µg/m³', 'openaq', 'Particle matter smaller 10µm'),
           ('co', 'µg/m³', 'CO', 'µg/m³', 'openaq', 'Carbon monoxide in µg/m³'),
           ('no2', 'µg/m³', 'NO2', 'µg/m³', 'openaq', 'Nitrogene dioxide in µg/m³'),
           ('so2', 'µg/m³', 'SO2', 'µg/m³', 'openaq', 'Sulfur dioxide in µg/m³')
        ON CONFLICT (orig_kpi_name, orig_unit, source) DO NOTHING ;
        """)

    # Get station data from facts
    # Elevation information not available in this dataset.
    # Future addition: Get rough elevation from 3rd party sources
    load_openaq_stations = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.d_stations
        SELECT
            -- not ideal / quick hack + adding country to make hash-conflicts
            -- even less likely
            md5(location||city||country)||country as station_id,
            'openaq' as source,
            latitude::numeric,
            longitude::numeric,
            -99999.9 as elevation,
            country as country_id, -- openaq country-field is varchar(2)
            left(location,30) as name
        FROM {OPENAQ_STAGING_SCHEMA}.f_air_data_raw
        WHERE left(date_utc,10) >= '{OPENAQ_DATA_AVAILABLE_FROM}'
        GROUP BY station_id, source, latitude, longitude, elevation, country_id, name
        ON CONFLICT (station_id, source) DO NOTHING
        ;
        """)

    # The inner join with *d_kpi_names* returns only records for KPIs that
    # are mapped in *d_kpi_names*
    # Aggregates per day to reduce data volume and make data more "digestable"
    # Data can contain "redeliveries" from past dates, hence make sure to
    # filter out data that goes beyond defined history period
    # Future additions: min & max values and extension of the d_kpi_names table
    # to account for different aggregation schemes
    transform_openaq_avg_facts = (f"""
        INSERT INTO {PRODUCTION_SCHEMA}.f_climate_data (
            station_id, source, date_,
            common_kpi_name, common_unit, data_value,
            observ_time
        )
        SELECT
            md5(location||city||country)||country as station_id,
            'openaq' as source,
            to_date(left(raw.date_utc,10),'YYYY-MM-DD') as date_,
            kpi.common_kpi_name,
            kpi.common_unit,
            avg(raw.value::numeric) as data_value,
            null as observ_time
        FROM {OPENAQ_STAGING_SCHEMA}.f_air_data_raw as raw
        JOIN {PRODUCTION_SCHEMA}.d_kpi_names as kpi
        ON raw.parameter = kpi.orig_kpi_name AND raw.unit = kpi.orig_unit
        WHERE left(raw.date_utc,10) >= '{OPENAQ_DATA_AVAILABLE_FROM}' AND
              raw.averaging_unit = 'hours' AND
              raw.averaging_value = '1'
        GROUP BY station_id, source, date_, kpi.common_kpi_name, kpi.common_unit
        ON CONFLICT (station_id, date_, common_kpi_name, common_unit) DO NOTHING
        ;
        """)

    delete_openaq_staging_data = (f"""
        BEGIN ;
        TRUNCATE TABLE {OPENAQ_STAGING_SCHEMA}.f_air_data_raw ;
        END ;
        """)

    #
    # ************** Common *************
    #

    aggregate_ger_monthly_data = (f"""
        CREATE TABLE IF NOT EXISTS {PRODUCTION_SCHEMA}.ol_mthly_analytic_ger AS
        SELECT
            f_agg.station_id,
            month,
            max(CASE WHEN common_kpi_name = 'TMAX' THEN max_data_value ELSE -9999 END) as max_tmax_c,
            max(CASE WHEN common_kpi_name = 'TMIN' THEN min_data_value ELSE -9999 END)  as min_tmin_c,
            max(CASE WHEN common_kpi_name = 'TMAX' THEN avg_data_value ELSE -9999 END)  as avg_tmax_c,
            max(CASE WHEN common_kpi_name = 'TMIN' THEN avg_data_value ELSE -9999 END)  as avg_tmin_c,
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
            GROUP BY station_id, month, common_kpi_name) as f_agg
        JOIN {PRODUCTION_SCHEMA}.d_stations as st
        ON f_agg.station_id = st.station_id
        WHERE d.country_id = 'GM'
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
