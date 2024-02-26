print("hello")

import pandas as pd
import requests
r = requests.get("https://api.data.gov.sg/v1/environment/air-temperature")
json_dict = r.json()
if json_dict["api_info"]["status"] == "healthy":
    #E.g. '2024-02-26T21:40:00+08:00'
    # Get temp data
    d = json_dict["items"][0]
    timestamp = d["timestamp"]
    readings = d["readings"]
    def reading_dict_pair_to_array_pair(array,timestamp):
        #e.g. [{'station_id': 'S117', 'value': 28.6}, {'station_id': 'S50', 'value': 27.7}] -> [["S117",28.6],["S50",27.7]]
        def helper(dict_pair):
            return [dict_pair["station_id"],round(dict_pair["value"],2),timestamp]
        return list(map(helper,array))
    df_readings = pd.DataFrame(reading_dict_pair_to_array_pair(readings,timestamp), columns=["station_id","value","timestamp"])
    #Get station data
    station_array = json_dict["metadata"]["stations"]
    def station_dict_to_array(array):
        #e.g. [{'id': 'S117', 'device_id': 'S117', 'name': 'Banyan Road', 'location': {'latitude': 1.256, 'longitude': 103.679}}, ...]
        def helper(dict):
            device_id = dict["device_id"]
            station_name = dict["name"]
            latitude = dict["location"]["latitude"]
            longitude = dict["location"]["longitude"]
            return [device_id,station_name,latitude,longitude]
        return list(map(helper, array))
    df_stations = pd.DataFrame(station_dict_to_array(station_array),columns=["device_id","station_name","latitude","longitude"])
    #Combine dfs and do a bit of cleaning
    df_merged = df_readings.merge(df_stations,how = "inner",left_on = "station_id",right_on="device_id")
    df_merged["timestamp"] = pd.to_datetime(df_merged["timestamp"],utc=True).dt.tz_convert(None)
    df_merged["date"] = df_merged["timestamp"].dt.date
    df_merged["time"] = df_merged["timestamp"].dt.time
    df_final = df_merged[["station_id","station_name","value","latitude","longitude","date","time"]].rename(columns = {"value":"temperature"})
    #Load data to PostgresSQL

else:
    #Query failed
    pass
print("hello")