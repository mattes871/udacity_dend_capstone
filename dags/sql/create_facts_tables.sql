--
-- noaa_staging schema
--
CREATE SCHEMA IF NOT EXISTS noaa_staging ;

-- No PRIMARY KEY definition to be able to deal with duplicate keys
-- (id+date_+element) once the data is already in Postgres
CREATE TABLE IF NOT EXISTS noaa_staging.f_weather_data_raw (
    id varchar(11) NOT NULL,
    date_ varchar(8) NOT NULL,
    element varchar(4),             -- Name of KPI measured
    data_value varchar(5),
    m_flag varchar(1),              -- Code on how measurement was done
    q_flag varchar(1),              -- quality flag
    s_flag varchar(1),              -- Indicates Source of measurement
    observ_time varchar(4)          -- local time of observation
);
CREATE INDEX IF NOT EXISTS f_weather_data_raw_ind
    ON noaa_staging.f_weather_data_raw(id, date_, element) ;


--
-- production schema
--
CREATE SCHEMA IF NOT EXISTS production ;

CREATE TABLE IF NOT EXISTS production.f_climate_data (
    unique_id varchar(16) NOT NULL, -- source-prefix + source-id
    source varchar(4) NOT NULL,
    -- country_id varchar(2) NOT NULL,
    date_ date NOT NULL,
    common_kpi_name varchar(4),     -- harmonized kpi_name, using a lookup table
    data_value integer,             -- in noaa only integer values
    observ_time varchar(4),
    PRIMARY KEY (unique_id, date_, common_kpi_name)
)   PARTITION BY RANGE (date_)
;


-- Provide a table with mapping data for the kpi names For the time being, only
-- NOAA data is loaded and the mapping applied is the identity mapping
--DROP TABLE IF EXISTS production.d_kpi_names;
CREATE TABLE IF NOT EXISTS production.d_kpi_names (
    orig_kpi_name   varchar(16) NOT NULL,
    common_kpi_name varchar(16) NOT NULL,
    source          varchar(8),
    description     text,
    PRIMARY KEY (orig_kpi_name)         -- Constraint necessary for 'ON CONFLICT'
);


-- Use only the 5 most relevant KPIs from NOAA, 65 additional KPIs can be found
-- in that source.
INSERT INTO production.d_kpi_names (
    orig_kpi_name, common_kpi_name, source, description)
    VALUES
           ('PRCP', 'PRCP', 'noaa', 'Precipitation (tenths of mm)'),
           ('SNOW', 'SNOW', 'noaa', 'Snowfall (mm)'),
           ('SNWD', 'SNWD', 'noaa', 'Snow depth (mm)'),
           ('TMAX', 'TMAX', 'noaa', 'Maximum temperature (tenths of degrees C)'),
           ('TMIN', 'TMIN', 'noaa', 'Minimum temperature (tenths of degrees C)')
    ON CONFLICT (orig_kpi_name) DO NOTHING
;
