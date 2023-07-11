from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import numpy as np

import dash_bootstrap_components as dbc

from database_init import create_database_if_not_exist, create_tables_if_not_exist, fetch_data
# Create the database and tables if they do not exist
create_database_if_not_exist()
create_tables_if_not_exist()

app = Dash(__name__, external_stylesheets=[dbc.themes.LUX])

# Define the layout
app.layout = html.Div(
    children=[
        html.H1(
            children="Velib station map",
            style={
                "font_family": "cursive",
                "textAlign": "center"
            }
        ),
        html.Div(
            children="The amount of velib not parked",
            style={
                "font_family": "cursive",
                "textAlign": "center"
            }
        ),
        html.Div(
            id="velib-not-parked",
            style={
                "font_family": "cursive",
                "textAlign": "center"
            }
        ),
        html.Div(
            children="Last update time",
            style={
                "font_family": "cursive",
                "textAlign": "center"
            }
        ),
        html.Div(
            id="last-update",
            style={
                "font_family": "cursive",
                "textAlign": "center"
            }
        ),
        dcc.Graph(id="velib-map", style={"height": "800px", "width": "100%"}),
        dcc.Interval(
            id="data-refresh-interval",
            interval=10000,  # Refresh the data every 10 seconds (adjust as needed)
            n_intervals=0
        )
    ]
)


# Callback to update the data periodically
@app.callback(
    [
        Output("velib-not-parked", "children"),
        Output("last-update", "children"),
        Output("velib-map", "figure")
    ],
    Input("data-refresh-interval", "n_intervals")
)
def update_data(n):
    # Fetch the latest data from the database
    df = fetch_data()

    # Perform any necessary data transformations

    # Calculate velib not parked
    velib_not_parked = np.sum(df["capacity"] - df["numdocksavailable"])
    df["last_reported"] = pd.to_datetime(df['last_reported'],unit='s',utc=True)

    df["last_reported"] = df["last_reported"].dt.tz_convert("Europe/Paris")
    # Get the last update time
    last_update = df["last_reported"].max().strftime("%H:%M %d/%m/%Y")

    # Generate the map figure
    fig = px.scatter_mapbox(
        df,
        lat="lat",
        lon="lon",
        color="numbikesavailable",
        size="capacity",
        color_continuous_scale="haline",
        size_max=15,
        zoom=10,
        hover_name="name",
        custom_data=["name", "capacity", "numbikesavailable"]
    )
    fig.update_layout(mapbox_style="open-street-map")

    return velib_not_parked, last_update, fig


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)
