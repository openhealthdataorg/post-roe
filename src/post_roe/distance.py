import pandas as pd
from post_roe.query import BucketQuery as bq
from geopy.distance import geodesic
import requests


GOOGLE_DISTANCE_API_KEY = "AIzaSyARPGbw0525MOHKf5l4hE41Z93lsp2L-8k"

class Distance:

    # get distances from origin-destination "o-d" pairs

    @staticmethod
    def geodesic(o: tuple, d: tuple) -> int:
        return int(geodesic(o, d).miles)

    @staticmethod
    def drive_time(o: tuple, d: tuple) -> int:
        def _get_minutes(google_distance_response: dict) -> int:
            try:
                seconds = google_distance_response["rows"][0]["elements"][0][
                    "duration"
                ]["value"]
                minutes = seconds / 60
                return int(minutes)
            except Exception as e:
                # print(e, google_distance_response)
                pass

        def _call_google(o: tuple, d: tuple) -> dict:
            def _tuple_to_string(x: tuple) -> str:
                return f"{x[0]},{x[1]}"

            params = {
                "origins": _tuple_to_string(o),
                "destinations": _tuple_to_string(d),
                "units": "imperial",
                "key": GOOGLE_DISTANCE_API_KEY,
            }
            url = "https://maps.googleapis.com/maps/api/distancematrix/json"
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(response.status_code, response.text)
            return response.json()

        data = _call_google(o, d)
        return _get_minutes(data)


origins = bq.census_zip3_query()
origins["lat_lon"] = origins.apply(lambda x: (x["lat"], x["lon"]), axis=1)

destination = bq.census_zip5_query().sample(1000).reset_index(drop=True)
destination["lat_lon"] = destination.apply(lambda x: (x["lat"], x["lon"]), axis=1)

def get_drive_time_mean(origin, k=5):
    df = destination[["lat_lon"]].reset_index()
    df["geodesic"] = df["lat_lon"].apply(
        lambda d: Distance.geodesic(origin["lat_lon"], d)
    )
    df = df.sort_values("geodesic")[0:k]

    df["drive_time"] = df["lat_lon"].apply(
        lambda d: Distance.drive_time(origin["lat_lon"], d)
    )
    # display(df)
    return df["drive_time"].mean()