import requests
import google.auth.transport.requests
import google.oauth2.id_token

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/parker/.gcloud/ohdo-post-roe-359822-keyfile.json'

API_BASE = "https://us-central1-ohdo-post-roe-359822.cloudfunctions.net"

def _authenticated_post(endpoint, json_params):
    url = f"{API_BASE}/{endpoint}"
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
    headers = {"Authorization": f"Bearer {id_token}"}
    resp = requests.post(url, json=json_params, headers=headers)
    return resp