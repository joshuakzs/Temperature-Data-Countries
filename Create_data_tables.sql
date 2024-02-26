CREATE TABLE IF NOT EXISTS raw_sg_data (
        station_id SERIAL,
        station_name VARCHAR(50),
        temperature NUMERIC(7,5),
        latitude NUMERIC(7,5),
        longitude NUMERIC(7,5),
        date_queried DATE,
        time_queried TIME
);
        