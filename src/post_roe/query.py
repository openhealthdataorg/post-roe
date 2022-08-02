import pandas as pd


DATA_BASE = "https://storage.googleapis.com/www.postroemap.org/data"

class BucketQuery: 

    # def __init__(self) -> None:
    #     self.zip_5 = self.census_zip5_query()
    #     self.states = self.states_query()

    @staticmethod
    def census_zip5_query() -> pd.DataFrame:
        df = pd.read_feather(f"{DATA_BASE}/census_geo_zip5_adi.feather")
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
    def census_zip3_query(states = []) -> pd.DataFrame:
        """
            Rollup zip5 to zip3 centroids
        """
        df = CloudQuery.census_zip5_query()
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
    def states_query(status=["protected","not_protected"]) -> pd.DataFrame:
        """
        state: str # 2 digit
        population: int
        _status_wp: str 4 status_buckets
        _status: two buckets: protected, not_protected
        """
        states = pd.read_csv(f"{DATA_BASE}/wp_roe_data.csv")  # washington post
        states = states[["States", "DATAWRAPPER"]]
        states = states.rename(columns={"DATAWRAPPER": "_status_wp", "States": "state"})
        state_populations = (
            CloudQuery.census_zip5_query()
            .groupby(["state"])
            .agg(census_total=("population", "sum"))
            .reset_index()
        )
        states = states.merge(state_populations)

        def _classify(status_wp: str) -> str:
            if status_wp == "Legal and likely to be protected":
                return "protected"
            else:
                return "not_protected"

        states["_status"] = states["_status_wp"].apply(_classify)
        states = states[states["_status"].isin(status)]
        states = states.sort_values("_status", ascending=False).reset_index(drop=True)
        return states

    @staticmethod
    def synethic_clinics_query(n=1000) -> pd.DataFrame:
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
        protected_states = CloudQuery.states_query(status=["protected"])
        census_zip5 = CloudQuery.census_zip5_query()
        protected_zip5 = census_zip5["state"].isin(protected_states["state"])
        clinics = protected_zip5.sample(n).reset_index(drop=True)
        return clinics[["state","zip5","lat","lon","population","adi_median"]]
