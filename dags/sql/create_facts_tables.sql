DROP TABLE IF EXISTS public.weather_data_raw ;
CREATE TABLE IF NOT EXISTS public.weather_data_raw (
    id varchar(11) NOT NULL,
    date_ varchar(8) NOT NULL,
    element varchar(4),
    data_value varchar(5),
    m_flag varchar(1),
    q_flag varchar(1),
    s_flag varchar(1),
    observ_time varchar(4),
    PRIMARY KEY (id, date_, element)
);


