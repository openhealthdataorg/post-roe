from post_roe.query import BucketQuery as bq

def seed_clinics(case="base", n=1000) -> None:
    protected_states = bq.states_query(case=case, status=["protected"])["state"]
    destinations = bq.census_zip5_query(states=protected_states)
    clinics = destinations.sample(n, weights="population").reset_index(drop=True)
    bq.to_feather(clinics, f"clinics_{case}")
    

seed_clinics("base")
seed_clinics("alt")

