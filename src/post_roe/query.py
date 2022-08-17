import pandas as pd

BUCKET_BASE = "https://storage.googleapis.com/www.postroemap.org/data"

import pandas as pd



class TinyQuery:
    
    DATA_BASE = "/Users/parker/Development/post-roe/data/"

    @staticmethod
    def adi_with_status_query() -> pd.DataFrame:
        states = pd.read_csv(f"{TinyQuery.DATA_BASE}/tf/_220805_wp_roe_data.csv")
        states = states[['state','state_status','_status_wp']]
        adi = pd.read_feather("/Users/parker/Development/post-roe/data/tf/adi_zip5.feather")
        adi = adi.merge(states, how='left').dropna(subset='state_status')
        adi['_adi_decile'] = (adi['adi_median'] / 10).astype(int)
        adi['_adi_decile'] = adi['_adi_decile'].apply(lambda x: 9 if x==10 else x)
        return adi

class BucketQuery:
    @staticmethod
    def census_zip5_query() -> pd.DataFrame:
        df = pd.read_feather(f"{BUCKET_BASE}/census_geo_zip5_adi.feather")
        df = df.rename(
            columns={
                "_lat": "lat",
                "_lng": "lon",
                "_state": "state",
                "_zip5": "zip5",
                "_census_total": "population",
            }
        )
        return df

    @staticmethod
    def census_zip3_query(states=[]) -> pd.DataFrame:
        """
        Rollup zip5 to zip3 centroids
        """
        df = BucketQuery.census_zip5_query()
        if len(states) > 0:
            df = df[df["state"].isin(states)]

        df["_zip3"] = df["_zip5"].apply(lambda x: f"{x[0:3]}**")
        census_zip3 = (
            df.groupby(["state", "_zip3"])
            .agg(
                lat=("lat", "mean"),
                lon=("lon", "mean"),
                population=("population", "sum"),
                adi_median=("adi_median", "median"),
            )
            .reset_index()
        )
        return census_zip3

    @staticmethod
    def states_query(
        state_codes=[], status=["protected", "not_protected"]
    ) -> pd.DataFrame:
        """
        state: str # 2 digit
        population: int
        _status_wp: str 4 status_buckets
        _status: two buckets: protected, not_protected
        """
        
        
        # states = pd.read_csv(f"{BUCKET_BASE}/wp_roe_data.csv")  # washington post
        # states = states[["States", "DATAWRAPPER"]]
        # states = states.rename(columns={"DATAWRAPPER": "_status_wp", "States": "state"})
        # state_populations = (
        #     BucketQuery.census_zip5_query()
        #     .groupby(["state"])
        #     .agg(census_total=("population", "sum"))
        #     .reset_index()
        # )
        # states = states.merge(state_populations)

        # def _classify(status_wp: str) -> str:
        #     if status_wp == "Legal and likely to be protected":
        #         return "protected"
        #     else:
        #         return "not_protected"

        # states["_status"] = states["_status_wp"].apply(_classify)
        # states = states[states["_status"].isin(status)]
        # if len(state_codes) > 0:
        #     states = states[states["state"].isin(state_codes)]
        # states = states.sort_values("_status", ascending=False).reset_index(drop=True)
        states = pd.read_csv("/Users/parker/Development/post-roe/data/tf/_220805_wp_roe_data.csv")
        return states

    @staticmethod
    def synethic_clinics_query(protected_states: list = [], n=1000) -> pd.DataFrame:
        """
        Stand in Sample Method until I get the actual clinic locations.
        This grabs 1k random zip codes in protected states.

        Why synthetic locations and what is a syntethic?
        Syntethic = statistically simular to the parent Population

        Why synthetic? Publishing lists widely has had to
        historically consider the increased safety risks to
        practitioners, and this project does not need to
        know the specific zip codes to understand a statistical
        model of the geometries around these geolocations.

        """
        census_zip5 = BucketQuery.census_zip5_query()

        if len(protected_states) > 0:
            protected_zip5 = census_zip5["state"].isin(protected_states)
        else:
            protected_states = BucketQuery.states_query(status=["proteced"])
            protected_zip5 = census_zip5["state"].isin(protected_states['state'])
        
        clinics = protected_zip5.sample(n).reset_index(drop=True)
        return clinics[["state", "zip5", "lat", "lon", "population", "adi_median"]]
