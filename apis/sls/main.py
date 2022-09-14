import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor

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
  k_closest_destinations = destinations.sort_values("geodesic")[0:k] 
  # 2. find the drive time
  k_closest_destinations["drive_time"] = k_closest_destinations["lat_lon"].apply(
      lambda d: Distance.drive_time(origin["lat_lon"], d)
  )
  drive_time_mean = float(k_closest_destinations["drive_time"].mean())  
  
  resp = {
    "zip3": origin['zip3'],
    "case": case,
    "drive_time": drive_time_mean
  }
  return resp
    
def process_async(request):
    """  
        Takes about 5 min at k=5, clinics = 1k
    """
    MAX_WORKERS = 20

    def _invoke_get_drive_time(origin: dict) -> dict:
        API_BASE = "https://us-central1-ohdo-post-roe-359822.cloudfunctions.net"
        endpoint = f"{API_BASE}/post-roe-sls-dev-get-drive-time"
        params = {
            "case": "base",
            "origin": {
                "lat_lon": list(origin['lat_lon']),
                "zip3": origin['zip3'],
            } 
        }
        resp = requests.post(endpoint, json=params)
        if resp.status_code != 200:
            print(resp.status_code, resp.text)
        else:
            return resp.json()
    
    # loads 8e2 zip3 origins
    origins = bq.census_zip3_query() 
    zip3_origins = origins[['zip3','lat_lon']].to_dict(orient="records")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
        futures = exec.map(_invoke_get_drive_time, zip3_origins)
    
    df = pd.DataFrame(list(futures))
    bq.to_feather(df) # this is brittle. errr
    
    return df