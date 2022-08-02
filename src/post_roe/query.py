import pandas as pd


DATA_BASE = "https://storage.googleapis.com/www.postroemap.org/data"


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


def census_zip3_query() -> pd.DataFrame:
    """
        Rollup zip5 to zip3 centroids
    """
    df = census_zip5_query()
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


def states_query(status="protected") -> pd.DataFrame:
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
        census_zip5_query()
        .groupby(["_state"])
        .agg(census_total=("_census_total", "sum"))
        .reset_index()
    )
    states = states.merge(state_populations)

    def _classify(status_wp: str) -> str:
        if status_wp == "Legal and likely to be protected":
            return "protected"
        else:
            return "not_protected"

    states["_status"] = states["_status_wp"].apply(_classify)
    states = states[states["_status"] == status]
    states = states.sort_values("_status", ascending=False).reset_index(drop=True)
    return states


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
    protected_states = states_query(status="protected")
    census_zip5 = census_zip5_query()
    protected_zip5 = census_zip5["_state"].isin(protected_states["_state"])
    clinics = protected_zip5.sample(n).reset_index(drop=True)
    return clinics[["state","zip5","lat","lon","population","adi_median"]]
