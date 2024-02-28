"""
Script Purpose: Collect temperature data from data.gov.sg using API. Load into local Postgres SQL database.

Concepts practiced:
Decorators, API, ETL, Functional Programming,
if __name__ main paradigm, Connecting and Interacting with local SQL Database
"""

def main():
    import pandas as pd
    import requests
    import psycopg2
    import psycopg2.extras as extras

    def print_size_of_result(function):
        def wrapper(*args,**kwargs):
            array = function(*args,**kwargs)
            print(f"We got {len(array)} new rows")
            return array
        return wrapper
    @print_size_of_result
    def reading_dict_pair_to_array_pair(array, timestamp):
        # e.g. [{'station_id': 'S117', 'value': 28.6}, {'station_id': 'S50', 'value': 27.7}] -> [["S117",28.6],["S50",27.7]]
        def helper(dict_pair):
            return [int(dict_pair["station_id"][1:]), round(dict_pair["value"], 2), timestamp]
        print("Get temperature data")
        return list(map(helper, array))
    @print_size_of_result
    def station_dict_to_array(array):
        #e.g. [{'id': 'S117', 'device_id': 'S117', 'name': 'Banyan Road', 'location': {'latitude': 1.256, 'longitude': 103.679}}, ...]
        def helper(dict):
            device_id = int(dict["device_id"][1:])
            station_name = dict["name"]
            latitude = dict["location"]["latitude"]
            longitude = dict["location"]["longitude"]
            return [device_id,station_name,latitude,longitude]
        print("Get station data")
        return list(map(helper, array))

    def get_timestamp(json_dict):
        return json_dict["items"][0]["timestamp"]

    def get_temperature_data(json_dict,timestamp):
        readings = json_dict["items"][0]["readings"]
        df_readings = pd.DataFrame(reading_dict_pair_to_array_pair(readings, timestamp),
                                   columns=["station_id", "value", "timestamp"])
        return df_readings

    def get_station_data(json_dict):
        station_array = json_dict["metadata"]["stations"]
        df_stations = pd.DataFrame(station_dict_to_array(station_array),columns=["station_id","station_name","latitude","longitude"])
        return df_stations

    def clean_df_readings(df_readings):
        df_readings["timestamp"] = pd.to_datetime(df_readings["timestamp"], utc=False).dt.tz_convert("Singapore")
        df_readings["date_queried"] = df_readings["timestamp"].dt.date
        df_readings["time_queried"] = df_readings["timestamp"].dt.time
        df_readings = df_readings[["station_id", "value", "date_queried", "time_queried"]].rename(
            columns={"value": "temperature"})
        return df_readings

    def execute_values(conn, df, table):
        #To load dataframe to postgresSQL database
        tuples = [tuple(x) for x in df.to_numpy()]

        cols = ','.join(list(df.columns))
        # SQL query to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("the dataframe is inserted")
        cursor.close()

    r = requests.get("https://api.data.gov.sg/v1/environment/air-temperature")
    json_dict = r.json()
    conn = psycopg2.connect(host="localhost",
                                dbname="tempdataetl",
                                user="postgres",
                                port=5432,
                                password="TempProjectETL")

    if json_dict["api_info"]["status"] == "healthy":
        timestamp = get_timestamp(json_dict) #E.g. '2024-02-26T21:40:00+08:00'
        df_readings = get_temperature_data(json_dict,timestamp)
        df_stations = get_station_data(json_dict)

        #Do a bit of cleaning
        df_readings = clean_df_readings(df_readings)
        print(f"Number of new temperature entries: {df_readings.shape[0]}")

        #Load data to PostgresSQL
        execute_values(conn, df_readings, 'raw_sg_data')
        execute_values(conn, df_stations, 'sg_station')
    else:
        #Query failed
        print("API request brought in 'unhealthy' data.")
        raise Exception

if __name__ == "__main__":
    main()