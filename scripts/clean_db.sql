DROP TABLE IF EXISTS noaa_staging.ghcnd_inventory_raw CASCADE ;
DROP TABLE IF EXISTS noaa_staging.ghcnd_stations_raw  CASCADE ;
DROP TABLE IF EXISTS noaa_staging.ghcnd_countries_raw  CASCADE ;
DROP TABLE IF EXISTS noaa_staging.f_weather_data_raw  CASCADE ;

DROP TABLE IF EXISTS production.d_inventory  CASCADE ;
DROP TABLE IF EXISTS production.d_stations  CASCADE ;
DROP TABLE IF EXISTS production.d_countries  CASCADE ;
DROP TABLE IF EXISTS production.f_climate_data  CASCADE ;
DROP TABLE IF EXISTS production.d_kpi_names CASCADE ;

DROP SCHEMA IF EXISTS openaq_staging CASCADE ;
