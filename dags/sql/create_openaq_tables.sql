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
    value varchar,
    unit varchar, -- measurand_unit
    location varchar, -- site_name???
    city varchar,
    country varchar,
    latitude varchar,
    longitude varchar,
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
