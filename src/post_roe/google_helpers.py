import pandas as pd

def _get_google_drive_distance(google_distance_response: dict) -> int:
    """
        get driving miles
    """
    try:
        meters = google_distance_response['rows'][0]['elements'][0]['distance']['value']
        miles = meters / 1609
        return int(miles)
    except Exception as e:
        # print(e, google_distance_response)
        pass

def _get_google_drive_duration(google_distance_response: dict) -> int:
    """
        get driving minute
    """
    try:
        seconds = google_distance_response['rows'][0]['elements'][0]['duration']['value']
        minutes = seconds / 60
        return int(minutes)
    except Exception as e:
        # print(e, google_distance_response)
        pass

def _get_destination_state(distance_matrix_response):
    try:
        destination = distance_matrix_response['destination_addresses'][0]
        destination_state = destination.split(",")[2].strip().split(" ")[0]
        return destination_state
    except:
        pass

def _get_origin_state(distance_matrix_response):
    try: 
        origin = distance_matrix_response['origin_addresses'][0]
        origin_state = origin.split(",")[2].strip().split(" ")[0]
        return origin_state
    except:
        pass

def _get_origin_zip(distance_matrix_response):
    try: 
        origin = distance_matrix_response['origin_addresses'][0]
        origin_state = origin.split(",")[2].strip().split(" ")[-1]
        return origin_state
    except:
        pass

# def load_cache():
#     closest_clinics = pd.read_feather("/Users/parker/Development/post-roe/data/tf/google_distance_matrix.feather")
#     closest_clinics['_roundtrip_hours'] = closest_clinics['drive_duration'].apply(lambda x: x*2/60)
#     closest_clinics['destination_state'] = closest_clinics['_distance_matrix_response'].apply(_get_destination_state) # add upstream
#     closest_clinics['origin_state'] = closest_clinics['_distance_matrix_response'].apply(_get_origin_state) # add upstream
#     closest_clinics['zip5'] = closest_clinics['_distance_matrix_response'].apply(_get_origin_zip) # add upstream
#     return closest_clinics