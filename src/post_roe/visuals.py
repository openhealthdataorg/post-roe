import plotly.express as px
px.set_mapbox_access_token("pk.eyJ1IjoicGFycXVhciIsImEiOiJja3lpcXMycGUxbmF5MnBzZXVzMHBzaXl4In0.jz0tx-HTJWym8jWPa8lqiA")

from post_roe.query import BucketQuery as bq

def draw_status_treemap() -> None:
    """
    Source: Washington Post
    """
    states = bq.states_query()
    protected_pct = (
        states[states["_status"] == "protected"]["population"].sum()
        / states["population"].sum()
    )
    unprotected_pct = (
        states[states["_status"] == "not_protected"]["population"].sum()
        / states["population"].sum()
    )
    px.treemap(
        states,
        path=["status", "_status_wp", "state"],
        color="_status_wp",
        values="population",
        height=600,
        title=f"Abortion Protections Status by State | Scaled by Total Population | Not::Protected {'{:.2f}'.format(unprotected_pct)}%::{'{:.2f}'.format(protected_pct)}% <br><sup>Source: Washington Post",
    ).show(renderer="notebook")