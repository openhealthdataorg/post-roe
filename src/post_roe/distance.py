from geopy.distance import geodesic
import requests
from post_roe.query import BucketQuery as bq

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



# def _get_google_drive_distance(google_distance_response: dict) -> int:
#     """
#         get driving miles
#     """
#     try:
#         meters = google_distance_response['rows'][0]['elements'][0]['distance']['value']
#         miles = meters / 1609
#         return int(miles)
#     except Exception as e:
#         # print(e, google_distance_response)
#         pass

# def _get_google_drive_duration(google_distance_response: dict) -> int:
#     """
#         get driving minute
#     """
#     try:
#         seconds = google_distance_response['rows'][0]['elements'][0]['duration']['value']
#         minutes = seconds / 60
#         return int(minutes)
#     except Exception as e:
#         # print(e, google_distance_response)
#         pass

# def _get_destination_state(distance_matrix_response):
#     try:
#         destination = distance_matrix_response['destination_addresses'][0]
#         destination_state = destination.split(",")[2].strip().split(" ")[0]
#         return destination_state
#     except:
#         pass

# def _get_origin_state(distance_matrix_response):
#     try: 
#         origin = distance_matrix_response['origin_addresses'][0]
#         origin_state = origin.split(",")[2].strip().split(" ")[0]
#         return origin_state
#     except:
#         pass

# def _get_origin_zip(distance_matrix_response):
#     try: 
#         origin = distance_matrix_response['origin_addresses'][0]
#         origin_state = origin.split(",")[2].strip().split(" ")[-1]
#         return origin_state
#     except:
#         pass

