from post_roe.helpers import _authenticated_post

def trigger_pipeline(case):
    json_params = {"case": case}
    resp = _authenticated_post("post-roe-sls-dev-process-async", json_params)
    if resp.status_code != 200:
        return dict(status_code = resp.status_code, text= resp.text)
    else:
        return resp.json()