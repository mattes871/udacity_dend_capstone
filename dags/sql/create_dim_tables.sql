CREATE TABLE IF NOT EXISTS public.ghcnd_stations_raw (
    id varchar(11) NOT NULL,
    latitude varchar(8),
    longitutde varchar(8),
    elevation varchar(6),
    state varchar(2),
    name varchar(30),
    gsn_flag varchar(3),
    hcn_crn_flag varchar(3),
    wmo_id varchar(5)
);

CREATE TABLE IF NOT EXISTS public.ghcnd_inventory_raw (
    id varchar(11) NOT NULL,
    latitude varchar(8),
    longitutde varchar(8),
    kpi varchar(4),
    from_year varchar(4),
    until_year varchar(4)
);

CREATE TABLE IF NOT EXISTS public.ghcnd_countries_raw (
    country_id varchar(2) NOT NULL,
    country varchar(64)
);


