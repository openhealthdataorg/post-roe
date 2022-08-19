from post_roe.query import BucketQuery as bq
from post_roe.distance import Distance

def get_drive_time(request):
  request_json = request.get_json()
  origin = request_json['origin']
  case = request_json.get("case", "base")
  k = request_json.get("k", 5)

  # 0. load all the clinics
  destinations = bq.clinics_query(case=case)
  # 1. find k closest geodesic "as the crow flies"
  destinations["geodesic"] = destinations["lat_lon"].apply(
      lambda d: Distance.geodesic(origin["lat_lon"], d)
  )
  destinations = destinations.sort_values("geodesic")[0:k] 
  # 2. find the drive time
  destinations["drive_time"] = destinations["lat_lon"].apply(
      lambda d: Distance.drive_time(origin["lat_lon"], d)
  )
  drive_time_mean = float(destinations["drive_time"].mean())  
  
  
  resp = {
    "zip3": origin['zip3'],
    "case": case,
    "drive_time": drive_time_mean
  }
  return resp
    

# import requests
# from concurrent.futures import ThreadPoolExecutor

# def build(request):
#     """  
#         Takes about 5 min at k=5, clinics = 1k
#     """
#     def _invoke_get_drive_time(origin: dict) -> dict:
#         API_BASE = "https://us-central1-ohdo-post-roe-359822.cloudfunctions.net"
#         endpoint = f"{API_BASE}/post-roe-sls-dev-get-drive-time"
#         params = {
#             "scenario": "base",
#             "zip3": origin['zip3'],
#             "lat_lon": list(origin['lat_lon']),
#         }
#         resp = requests.post(endpoint, json=params)
#         if resp.status_code != 200:
#             print(resp.status_code, resp.text)
#         else:
#             return resp.json()
#     origins = bq.census_zip3_query()
#     zip3_origins = origins[['zip3','lat_lon']].to_dict(orient="records")
#     with ThreadPoolExecutor(max_workers=20) as exec:
#         futures = exec.map(_invoke_get_drive_time, zip3_origins)
#     df = pd.DataFrame(list(futures))
#     bq.to_feather(df)
#     return df