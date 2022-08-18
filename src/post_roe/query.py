import pandas as pd

BUCKET_BASE = "https://storage.googleapis.com/www.postroemap.org/data"

class BucketQuery:
    @staticmethod
    def census_zip5_query(states=[]) -> pd.DataFrame:
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
        df = df[~df['state'].isin(["HI","AK"])].reset_index(drop=True)
        if len(states) > 0:
            df = df[df['state'].isin(states)].reset_index(drop=True)
        return df

    @staticmethod
    def census_zip3_query(states=[]) -> pd.DataFrame:
        """
        Rollup zip5 to zip3 centroids
        """
        df = BucketQuery.census_zip5_query()
        if len(states) > 0:
            df = df[df["state"].isin(states)]

        df["zip3"] = df["zip5"].apply(lambda x: f"{x[0:3]}**")
        census_zip3 = (
            df.groupby(["state", "zip3"])
            .agg(
                lat=("lat", "mean"),
                lon=("lon", "mean"),
                population=("population", "sum"),
                adi_median=("adi_median", "median"),
            )
            .reset_index()
        )
        census_zip3["lat_lon"] = census_zip3.apply(lambda x: (x["lat"], x["lon"]), axis=1)
        return census_zip3

    @staticmethod
    def states_query(
        state_codes=[], status=["protected", "at_risk"]
    ) -> pd.DataFrame:
        """
        state: str # 2 digit
        population: int
        _status_wp: str 4 status_buckets
        _status: two buckets: protected, not_protected
        """
        states = pd.read_csv("/Users/parker/Development/post-roe/data/tf/_220805_wp_roe_data.csv")
        states = states[states["state_status"].isin(status)]
        return states

    @staticmethod
    def synthetic_clinics_query(n=1000, force=False) -> pd.DataFrame:
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
        cache_url = f"{BUCKET_BASE}/220818-synthetic-destinations.feather"
        if force:
            destinations = BucketQuery.census_zip5_query(
                states=BucketQuery.states_query(status=['protected'])['state']
            ) 
            destinations = destinations.sample(1000, weights='population')
            destinations["lat_lon"] = destinations.apply(lambda x: (x["lat"], x["lon"]), axis=1)
            destinations = destinations[["state", "zip5", "lat_lon", "population", "adi_median"]].reset_index(drop=True)
        else:
            destinations = pd.read_feather(cache_url)
        return destinations


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