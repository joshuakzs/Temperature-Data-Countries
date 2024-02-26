CREATE TABLE IF NOT EXISTS raw_sg_data (
        station_id SERIAL,
        temperature NUMERIC(7,5),
        date_queried DATE,
        time_queried TIME
);

CREATE TABLE IF NOT EXISTS sg_station (
    station_id SERIAL,
    station_name VARCHAR(50),
    latitude NUMERIC(13,7),
    longitude NUMERIC(13,7),
)
        