--
-- noaa_staging schema
--
CREATE SCHEMA IF NOT EXISTS noaa_staging ;


-- No PRIMARY KEY here, to be able to deal with duplicates once the data is
-- already in Postgres
CREATE TABLE IF NOT EXISTS noaa_staging.ghcnd_stations_raw (
    id varchar(11) NOT NULL,
    latitude varchar(9),
    longitude varchar(9),
    elevation varchar(6),
    state varchar(2),
    name varchar(30),
    gsn_flag varchar(3),
    hcn_crn_flag varchar(3),
    wmo_id varchar(5)
);
CREATE INDEX IF NOT EXISTS ghcnd_stations_raw_id ON noaa_staging.ghcnd_stations_raw(id) ;


-- No PRIMARY KEY here, to be able to deal with duplicates once the data is
-- already in Postgres
CREATE TABLE IF NOT EXISTS noaa_staging.ghcnd_inventory_raw (
    id varchar(11) NOT NULL,
    latitude varchar(9),
    longitude varchar(9),
    element varchar(4),
    from_year varchar(4),
    until_year varchar(4)
);
CREATE INDEX IF NOT EXISTS ghcnd_inventory_raw_id ON noaa_staging.ghcnd_inventory_raw(id) ;


-- No PRIMARY KEY here, to be able to deal with duplicates once the data is
-- already in Postgres
CREATE TABLE IF NOT EXISTS noaa_staging.ghcnd_countries_raw (
    country_id varchar(2) NOT NULL,
    country varchar(64)
);
CREATE INDEX IF NOT EXISTS ghcnd_countries_raw_id ON noaa_staging.ghcnd_countries_raw(country_id) ;

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
