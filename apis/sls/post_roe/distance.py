from geopy.distance import geodesic
import requests

GOOGLE_DISTANCE_API_KEY = "AIzaSyARPGbw0525MOHKf5l4hE41Z93lsp2L-8k"

class Distance:

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

        def _call_google_drive_time(o: tuple, d: tuple) -> dict:
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
        data = _call_google_drive_time(o, d)
        return _get_minutes(data)