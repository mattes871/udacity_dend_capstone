--
-- openaq_staging schema
--
CREATE SCHEMA IF NOT EXISTS openaq_staging ;


-- No PRIMARY KEY here, to be able to deal with duplicates once the data is
-- already in Postgres
CREATE TABLE IF NOT EXISTS openaq_staging.stations_raw (
    sensor_node_id varchar NOT NULL,
    site_name      varchar,
    node_source_name varchar,
    node_ismobile  varchar,
    node_latitude  varchar,
    node_longitude varchar,
    sensor_id varchar,
    measurand_parameter varchar,
    measurand_unit varchar
);
-- CREATE TABLE IF NOT EXISTS openaq_staging.stations_raw (
--     sensor_node_id varchar(32) NOT NULL,
--     site_name      varchar(32),
--     node_source_name varchar(16),
--     node_ismobile  varchar(5),
--     node_latitude  varchar(11),
--     node_longitude varchar(11),
--     sensor_id varchar(32),
--     measurand_parameter varchar(16),
--     measurand_unit varchar(16)
-- );
CREATE INDEX IF NOT EXISTS stations_raw_id ON openaq_staging.stations_raw(sensor_node_id) ;


-- No PRIMARY KEY definition to be able to deal with duplicate keys
-- DROP TABLE IF EXISTS  openaq_staging.f_air_data_raw CASCADE ;
CREATE TABLE IF NOT EXISTS openaq_staging.f_air_data_raw (
    date_utc varchar,
    date_local varchar,
    parameter varchar, --measurand_parameter
    location varchar, -- site_name???
    value varchar,
    unit varchar, -- measurand_unit
    latitude varchar,
    longitude varchar,
    country varchar,
    source_name varchar, --node_source_name
    source_type varchar,
    mobile varchar, -- node_ismobile
    averaging_unit varchar,
    averaging_value varchar
) ;
-- CREATE TABLE IF NOT EXISTS openaq_staging.f_air_data_raw (
--     date_utc varchar(25),
--     date_local varchar(25),
--     parameter varchar(16), --measurand_parameter
--     location varchar(32), -- site_name???
--     value varchar(8),
--     unit varchar(16), -- measurand_unit
--     latitude varchar(11),
--     longitude varchar(11),
--     country varchar(3),
--     source_name varchar(16), --node_source_name
--     source_type varchar(16),
--     mobile varchar(5), -- node_ismobile
--     averaging_unit varchar(9),
--     averaging_value varchar(5)
-- ) ;

-- CREATE INDEX IF NOT EXISTS f_air_data_raw
--     ON openaq_staging.f_air_data_raw(id, date_, element) ;


--
-- production schema
--
CREATE SCHEMA IF NOT EXISTS production ;

CREATE TABLE IF NOT EXISTS production.d_stations (
    station_id varchar(16) NOT NULL,
    source varchar(4) NOT NULL,
    latitude numeric (10,7),
    longitude numeric (10,7),
    elevation int,
    state varchar(2),
    name varchar(30),
    --gsn_flag varchar(3),
    --hcn_crn_flag varchar(3),
    --wmo_id varchar(5),
    PRIMARY KEY (station_id)
);

CREATE TABLE IF NOT EXISTS production.d_countries (
    country_id varchar(2) NOT NULL,
    country varchar(64),
    source varchar(4) NOT NULL,
    PRIMARY KEY (country_id, source)
);


CREATE TABLE IF NOT EXISTS production.f_airpoll_data (
    station_id varchar(16) NOT NULL, -- source-prefix + source-id
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
    common_kpi_name varchar(16) NOT NULL,
    source          varchar(4) NOT NULL,
    description     text,
    PRIMARY KEY (orig_kpi_name, source)   -- Constraint necessary for 'ON CONFLICT'
);


-- Use only the 5 most relevant KPIs from NOAA, 65 additional KPIs can be found
-- in that source.
-- INSERT INTO production.d_kpi_names (
--     orig_kpi_name, common_kpi_name, source, description)
--     VALUES
--            ('PRCP', 'PRCP', 'opaq', 'Precipitation (tenths of mm)'),
--            ('SNOW', 'SNOW', 'opaq', 'Snowfall (mm)'),
--            ('SNWD', 'SNWD', 'opaq', 'Snow depth (mm)'),
--            ('TMAX', 'TMAX', 'opaq', 'Maximum temperature (tenths of degrees C)'),
--            ('TMIN', 'TMIN', 'opaq', 'Minimum temperature (tenths of degrees C)')
--     ON CONFLICT (orig_kpi_name) DO NOTHING
-- ;
