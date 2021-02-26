class SqlQueries:

    most_recent_noaa_data = (""" 
        SELECT to_char(max(date_),'YYYY-MM-DD') as max_date_str 
              FROM public.f_climate_data 
              WHERE source = 'noaa' ;
        """)

    # Use only the 5 most relevant KPIs from NOAA, 65 additional KPIs can be found
    # in that source.
    populate_d_kpi_table = ("""
        INSERT INTO public.d_kpi_names (
            orig_kpi_name, common_kpi_name, source, description)
        VALUES
           ('PRCP', 'PRCP', 'noaa', 'Precipitation (tenths of mm)'),
           ('SNOW', 'SNOW', 'noaa', 'Snowfall (mm)'),
           ('SNWD', 'SNWD', 'noaa', 'Snow depth (mm)'),
           ('TMAX', 'TMAX', 'noaa', 'Maximum temperature (tenths of degrees C)'),
           ('TMIN', 'TMIN', 'noaa', 'Minimum temperature (tenths of degrees C)')
        ON CONFLICT (orig_kpi_name) DO NOTHING ;
        """)

    load_noaa_stations = (""" 
        INSERT INTO public.d_stations
        SELECT 
            'noaa' || id as unique_id,
            'noaa' as source,
            latitude, longitude, elevation,
            state, name
        FROM public.ghcnd_stations_raw
        WHERE id is not null
        ON CONFLICT (unique_id) DO NOTHING
        ;
        """)

    # The inner join with *d_kpi_names* returns only records for KPIs that
    # are mapped in *d_kpi_names*
    load_noaa_inventory = (""" 
        INSERT INTO public.d_inventory
        SELECT 
            'noaa' || raw.id as unique_id,
            'noaa' as source,
            kpi.common_kpi_name,
            raw.from_year as from_year,
            raw.until_year as until_year
        FROM public.ghcnd_inventory_raw as raw
        JOIN public.d_kpi_names as kpi
        ON raw.element = kpi.orig_kpi_name
        ON CONFLICT (unique_id, common_kpi_name) DO UPDATE
            SET (from_year, until_year) = (EXCLUDED.from_year,
                                           EXCLUDED.until_year)
        ;
        """)


    load_noaa_countries = (""" 
        INSERT INTO public.d_countries
        SELECT 
            country_id,
            country,
            'noaa' as source
        FROM public.ghcnd_countries_raw
        ON CONFLICT (country_id, source) DO NOTHING
        ;
        """)

    # The inner join with *d_kpi_names* returns only records for KPIs that
    # are mapped in *d_kpi_names*
    transform_noaa_facts = (""" 
        INSERT INTO public.f_climate_data
        SELECT 
            'noaa' || id as unique_id,
            'noaa' as source,
            to_date(raw.date_,'YYYYMMDD') as date_,
            kpi.common_kpi_name,
            raw.data_value::int as data_value,
            raw.observ_time as observ_time
        FROM public.f_weather_data_raw as raw
        JOIN public.d_kpi_names as kpi
        ON raw.element = kpi.orig_kpi_name
        ON CONFLICT (unique_id, date_, common_kpi_name) DO NOTHING
        ;
        """)

    def create_partition_table_cmd(schema: str,
                                   table: str,
                                   year: int) -> str:
        """ 
        Given the year, generate the SQL command for creating a partition table
        """
        f_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table}_{year} PARTITION OF {table}
            FOR VALUES FROM ('{year}-01-01') TO ('{year+1}-01-01') ;
            """
        return f_sql

    aggregate_ger_monthly_data = (""" 
        CREATE TABLE IF NOT EXISTS public.ol_mthly_analytic_ger AS
        SELECT 
            f_agg.unique_id,
            month,
            max(CASE WHEN common_kpi_name = 'TMAX' THEN max_data_value ELSE -9999 END) as max_tmax
            max(CASE WHEN common_kpi_name = 'TMIN' THEN min_data_value ELSE -9999 END) as min_tmin
            max(CASE WHEN common_kpi_name = 'TMAX' THEN avg_data_value ELSE -9999 END) as avg_tmax
            max(CASE WHEN common_kpi_name = 'TMIN' THEN avg_data_value ELSE -9999 END) as avg_tmin
            sum(CASE WHEN common_kpi_name = 'PRCP' THEN sum_data_value ELSE -9999 END) as sum_prcp
        FROM 
            (SELECT
                unique_id,
                common_kpi_name,
                date_trunc('month',date_) as month,
                avg(data_value) as avg_data_value,
                min(data_value) as min_data_value,
                max(data_value) as max_data_value,
                sum(data_value) as sum_data_value
            FROM public.f_climate_data
            WHERE substr(unique_id,5,2) = 'GM' -- Germany
            GROUP BY month, common_kpi_name) as f_agg
        JOIN public.d_stations as d
        ON f_agg.unique_id = d.unique_id
        GROUP BY f_agg.unique_id, month
        ;
        """)


