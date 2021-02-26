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


--
-- production schema
--
CREATE SCHEMA IF NOT EXISTS production ;

CREATE TABLE IF NOT EXISTS production.d_stations (
    unique_id varchar(16) NOT NULL,
    source varchar(4) NOT NULL,
    latitude varchar(9),
    longitude varchar(9),
    elevation varchar(6),
    state varchar(2),
    name varchar(30),
    --gsn_flag varchar(3),
    --hcn_crn_flag varchar(3),
    --wmo_id varchar(5),
    PRIMARY KEY (unique_id)
);

CREATE TABLE IF NOT EXISTS production.d_inventory (
    unique_id varchar(16) NOT NULL,
    source varchar(4) NOT NULL,
    common_kpi_name varchar(4) NOT NULL,
    from_year varchar(4),
    until_year varchar(4),
    PRIMARY KEY (unique_id, common_kpi_name)
);

CREATE TABLE IF NOT EXISTS production.d_countries (
    country_id varchar(2) NOT NULL,
    country varchar(64),
    source varchar(4) NOT NULL,
    PRIMARY KEY (country_id, source)
);
