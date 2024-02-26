CREATE TABLE IF NOT EXISTS raw_sg_data (
        station_id SERIAL,
        station_name VARCHAR(50),
        temperature NUMERIC(7,5),
        latitude NUMERIC(13,7),
        longitude NUMERIC(13,7),
        date_queried DATE,
        time_queried TIME
);
        