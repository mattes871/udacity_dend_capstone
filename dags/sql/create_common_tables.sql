--
-- production schema
--
CREATE SCHEMA IF NOT EXISTS production ;

CREATE TABLE IF NOT EXISTS production.d_stations (
    station_id varchar NOT NULL,
    source varchar NOT NULL,
    latitude numeric,
    longitude numeric,
    elevation numeric,
    state varchar(2),
    name varchar(30),
    --gsn_flag varchar(3),
    --hcn_crn_flag varchar(3),
    --wmo_id varchar(5),
    PRIMARY KEY (station_id, source)
);

CREATE TABLE IF NOT EXISTS production.d_inventory (
    station_id varchar NOT NULL,
    source varchar NOT NULL,
    common_kpi_name varchar(16) NOT NULL,
    common_unit varchar(8) NOT NULL,
    from_year varchar(4),
    until_year varchar(4),
    PRIMARY KEY (station_id, common_kpi_name, common_unit)
);

CREATE TABLE IF NOT EXISTS production.d_countries (
    country_id varchar(2) NOT NULL,
    country varchar,
    source varchar(4) NOT NULL,
    PRIMARY KEY (country_id, source)
);


CREATE TABLE IF NOT EXISTS production.f_climate_data (
    station_id varchar NOT NULL, -- source-prefix + source-id
    source varchar NOT NULL,
    -- country_id varchar(2) NOT NULL,
    date_ date NOT NULL,
    common_kpi_name varchar(16),     -- harmonized kpi_name, using a lookup table
    common_unit varchar(8),
    data_value numeric,             -- in noaa only integer values
    observ_time varchar(4),
    PRIMARY KEY (station_id, date_, common_kpi_name, common_unit)
)   PARTITION BY RANGE (date_)
;

CREATE TABLE IF NOT EXISTS production.f_airpoll_data (
    station_id varchar NOT NULL, -- source-prefix + source-id
    source varchar(4) NOT NULL,
    -- country_id varchar(2) NOT NULL,
    date_ date NOT NULL,
    common_kpi_name varchar(4),     -- harmonized kpi_name, using a lookup table
    data_value integer,             -- in openaq only integer values
    observ_time varchar(4),
    PRIMARY KEY (station_id, date_, common_kpi_name)
)   PARTITION BY RANGE (date_)
;



-- Provide a table with mapping data for the kpi names For the time being, only
-- NOAA data is loaded and the mapping applied is the identity mapping
--DROP TABLE IF EXISTS production.d_kpi_names;
CREATE TABLE IF NOT EXISTS production.d_kpi_names (
    orig_kpi_name   varchar(16) NOT NULL,
    orig_unit       varchar(8) NOT NULL,
    common_kpi_name varchar(16) NOT NULL,
    common_unit     varchar(8) NOT NULL,
    source          varchar NOT NULL,
    description     text,
    PRIMARY KEY (orig_kpi_name, orig_unit, source)  -- Constraint necessary for 'ON CONFLICT'
);


-- Use only the 5 most relevant KPIs from NOAA, 65 additional KPIs can be found
-- in that source.
-- INSERT INTO production.d_kpi_names (
--     orig_kpi_name, common_kpi_name, source, description)
--     VALUES
--            ('PRCP', 'PRCP', 'noaa', 'Precipitation (tenths of mm)'),
--            ('SNOW', 'SNOW', 'noaa', 'Snowfall (mm)'),
--            ('SNWD', 'SNWD', 'noaa', 'Snow depth (mm)'),
--            ('TMAX', 'TMAX', 'noaa', 'Maximum temperature (tenths of degrees C)'),
--            ('TMIN', 'TMIN', 'noaa', 'Minimum temperature (tenths of degrees C)')
--     ON CONFLICT (orig_kpi_name, source) DO NOTHING
-- ;
