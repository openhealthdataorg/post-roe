import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import requests

from post_roe.query import BucketQuery as bq, BUCKET_BASE

def build(case="base"):
    """  
        Takes about 5 min at k=5, clinics = 1k
    """
    def _invoke_get_drive_time(origin: dict) -> dict:
        API_BASE = "https://us-central1-ohdo-post-roe-359822.cloudfunctions.net"
        endpoint = f"{API_BASE}/post-roe-sls-dev-get-drive-time"
        params = {
            "case": case,
            "k": 5,
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
            
    origins = bq.census_zip3_query()
    zip3_origins = origins[['zip3','lat_lon']].to_dict(orient="records")
    with ThreadPoolExecutor(max_workers=20) as exec:
        futures = exec.map(_invoke_get_drive_time, zip3_origins)
    drive_time = pd.DataFrame(list(futures))
    
    # enrich
    df = origins.merge(drive_time, how="left").drop_duplicates(subset=['zip3']).drop(columns=["lat_lon"])
    df = df.merge(bq.states_query(case=case)[['state','state_status']])
    df['adi_decile'] = df['adi_median'].apply(lambda x: int(x/10))
    df['_drive_time'] = df['drive_time'].dropna().apply(lambda x: int(x/60))
    df.to_feather(f"drive_time_{case}.feather")
    # bq.to_feather(df) #cache
    return df
