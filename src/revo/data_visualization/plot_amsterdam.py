import pandas as pd
import plotly.express as px
from plotly.graph_objs._figure import Figure


def plot_amsterdam(x: pd.DataFrame, value: str, features: dict) -> Figure:
    """
    Plot the values of a given DataFrame as a map.

    :param x: DataFrame containing the dataset to be plotted.
    :param value: String defining which column in the dataset represents
           the z-axis.
    :param features: Dict with geometries to be plotted with zipcode
           property (pc4_code).
    :return: Map-visualization of the results.
    """

    fig = px.choropleth(
        x,
        geojson=features,
        locations="zipcode",
        featureidkey="properties.pc4_code",
        color=value,
        color_continuous_scale="Viridis",
        projection="mercator",
        labels={value: value.capitalize().replace("_", " ")},
    )

    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0}, width=600, height=350
    )

    return fig
