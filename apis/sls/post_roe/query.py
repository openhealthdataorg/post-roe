import pandas as pd

BUCKET_BASE = "https://storage.googleapis.com/ohdo-post-roe-dev/data"
BUCKET_GS = "gs://ohdo-post-roe-dev/data"

class BucketQuery:
   
    @staticmethod
    def to_feather(df: pd.DataFrame, fname: str) -> None:
        df.to_feather(f"{BUCKET_GS}/{fname}.feather")

    @staticmethod
    def census_zip5_query(case="base", states=[]) -> pd.DataFrame:
        df = pd.read_feather(f"{BUCKET_BASE}/census_geo_zip5_adi.feather")
        df["lat_lon"] = df.apply(lambda x: (x["lat"], x["lon"]), axis=1)
        if case == "base":
            df = df[~df["state"].isin(["HI", "AK"])].reset_index(drop=True)
        if len(states) > 0:
            df = df[df["state"].isin(states)].reset_index(drop=True)
        return df[["state", "zip5", "lat_lon", "population", "adi_median"]]

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
        census_zip3["lat_lon"] = census_zip3.apply(
            lambda x: (x["lat"], x["lon"]), axis=1
        )
        return census_zip3

    @staticmethod
    def states_query(case="base", status=["protected", "at_risk"]) -> pd.DataFrame:
        """
            state: str, population: int, status: enum(protected, at_risk)
        """
        states = pd.read_feather(f"{BUCKET_BASE}/state_status_base.feather")
        if case == "base":
            states["case"] = "base"
        elif case == "alt":
            ## swap GA status
            i = states.index[states["state"] == "GA"][0]
            states.at[i, "state_status"] = "protected"
            states["case"] = "alt"

        # filter
        states = states[states["state_status"].isin(status)]
        return states

    @staticmethod
    def clinics_query(case="base", n=1000) -> pd.DataFrame:
        df = pd.read_feather(f"{BUCKET_BASE}/clinics_{case}_{n}.feather") 
        return df