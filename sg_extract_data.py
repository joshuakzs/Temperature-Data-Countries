import pandas as pd
import requests
import psycopg2
import psycopg2.extras as extras

r = requests.get("https://api.data.gov.sg/v1/environment/air-temperature")
json_dict = r.json()
if json_dict["api_info"]["status"] == "healthy":
    # Get temp data
    d = json_dict["items"][0]
    timestamp = d["timestamp"] #E.g. '2024-02-26T21:40:00+08:00'
    readings = d["readings"]
    def reading_dict_pair_to_array_pair(array,timestamp):
        #e.g. [{'station_id': 'S117', 'value': 28.6}, {'station_id': 'S50', 'value': 27.7}] -> [["S117",28.6],["S50",27.7]]
        def helper(dict_pair):
            return [int(dict_pair["station_id"][1:]),round(dict_pair["value"],2),timestamp]
        return list(map(helper,array))
    df_readings = pd.DataFrame(reading_dict_pair_to_array_pair(readings,timestamp), columns=["station_id","value","timestamp"])
    #Get station data
    station_array = json_dict["metadata"]["stations"]
    def station_dict_to_array(array):
        #e.g. [{'id': 'S117', 'device_id': 'S117', 'name': 'Banyan Road', 'location': {'latitude': 1.256, 'longitude': 103.679}}, ...]
        def helper(dict):
            device_id = int(dict["device_id"][1:])
            station_name = dict["name"]
            latitude = dict["location"]["latitude"]
            longitude = dict["location"]["longitude"]
            return [device_id,station_name,latitude,longitude]
        return list(map(helper, array))
    df_stations = pd.DataFrame(station_dict_to_array(station_array),columns=["station_id","station_name","latitude","longitude"])
    #Do a bit of cleaning
    df_readings["timestamp"] = pd.to_datetime(df_readings["timestamp"],utc=False).dt.tz_convert("Singapore")
    df_readings["date_queried"] = df_readings["timestamp"].dt.date
    df_readings["time_queried"] = df_readings["timestamp"].dt.time
    df_readings = df_readings[["station_id","value","date_queried","time_queried"]].rename(columns = {"value":"temperature"})
    print(f"Number of new temperature entries: {df_readings.shape[0]}")

    #Load data to PostgresSQL
    conn = psycopg2.connect(host="localhost",
                            dbname="tempdataetl",
                            user="postgres",
                            port=5432,
                            password="TempProjectETL")
    def execute_values(conn, df, table):

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

    execute_values(conn, df_readings, 'raw_sg_data')
    execute_values(conn, df_stations, 'sg_station')
else:
    #Query failed
    print("API request brought in 'unhealthy' data.")
    raise Exception
print("hello")