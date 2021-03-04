--
-- production schema
--
CREATE SCHEMA IF NOT EXISTS production ;

CREATE TABLE IF NOT EXISTS production.d_stations (
    station_id varchar NOT NULL, -- Unique identifier for weather station
    source varchar NOT NULL,     -- Data source: {noaa, openaq}
    latitude numeric,            -- Geographical location
    longitude numeric,           -- Geographical location
    elevation numeric,           -- Elevation of station
    country_id varchar(2),       -- country id (=state in noaa dataset)
    name varchar(30),            -- Human readable station name
    --gsn_flag varchar(3),
    --hcn_crn_flag varchar(3),
    --wmo_id varchar(5),
    PRIMARY KEY (station_id, source)
);

CREATE TABLE IF NOT EXISTS production.d_inventory (
    station_id varchar NOT NULL, -- Unique identifier for weather station
                                 -- Requires 'source' to be unique
    source varchar NOT NULL,     -- Data source: {noaa, openaq}
    common_kpi_name varchar(16) NOT NULL, -- Harmonized KPI name
    common_unit varchar(8) NOT NULL,      -- Harmonized unit name
    from_year varchar(4),                 -- Data available since
    until_year varchar(4),                -- Data available until
    PRIMARY KEY (station_id, common_kpi_name, common_unit)
);

CREATE TABLE IF NOT EXISTS production.d_countries (
    country_id varchar(2) NOT NULL, -- Unique country id
    country varchar,                -- Country name as in source data
    source varchar(4) NOT NULL,     -- Data source: {noaa, openaq}
    PRIMARY KEY (country_id, source)
);


CREATE TABLE IF NOT EXISTS production.f_climate_data (
    station_id varchar NOT NULL, -- Unique identifier for weather station
                                 -- Requires 'source' to be unique
    source varchar NOT NULL,     -- Data source: {noaa, openaq}
    date_ date NOT NULL,         -- Date of measurement
    common_kpi_name varchar(16), -- harmonized kpi_name, using a lookup table
    common_unit varchar(8),      -- harmonized unit of measurement
    data_value numeric,          -- measured value (harmonized)
    observ_time varchar(4),      -- observation time if available
    PRIMARY KEY (station_id, date_, common_kpi_name, common_unit)
)   PARTITION BY RANGE (date_)
;


-- Provide a table with mapping data for the kpi names For the time being, only
-- NOAA data is loaded and the mapping applied is the identity mapping
--DROP TABLE IF EXISTS production.d_kpi_names;
CREATE TABLE IF NOT EXISTS production.d_kpi_names (
    orig_kpi_name   varchar(16) NOT NULL, -- KPI name in source
    orig_unit       varchar(8) NOT NULL,  -- Unit of measurement in source
    common_kpi_name varchar(16) NOT NULL, -- Harmonized KPI name
    common_unit     varchar(8) NOT NULL,  -- Harmonized unit of measurement
    source          varchar NOT NULL,     -- data source {noaa, openaq}
    description     text,                 -- description of KPI and unit
    PRIMARY KEY (orig_kpi_name, orig_unit, source)  -- Constraint necessary for 'ON CONFLICT'
);
