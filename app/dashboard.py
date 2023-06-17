from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import time
import psycopg2
import numpy as np
import seaborn as sns
import dash_bootstrap_components as dbc


conn = psycopg2.connect(
   database="velib", user='airflow', password='airflow', host='postgres', port= '5432'
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Executing an SQL request using the execute() method
cursor.execute("select * from velib_station")

# Fetch a single row using fetchone() method.
data = cursor.fetchall()

cols = []
for elt in cursor.description:
    cols.append(elt[0])
df = pd.DataFrame(data=data,columns=cols)

df["last_reported"] = pd.to_datetime(df['last_reported'],unit='s',utc=True)

df["last_reported"] = df["last_reported"].dt.tz_convert("Europe/Paris")

last_update = df["last_reported"].max().strftime("%H:%M %d/%m/%Y")

velib_not_parked = np.sum(df["capacity"]-df["numDocksAvailable"])



#Closing the connection
conn.close()

app = Dash(__name__,external_stylesheets=[dbc.themes.LUX])

fig = px.scatter_mapbox(df, lat="lat", lon="lon", color="numBikesAvailable",size="capacity",
                  color_continuous_scale="haline", size_max=15, zoom=10,hover_name="name",custom_data=["name","capacity","numBikesAvailable"])
fig.update_layout(mapbox_style="open-street-map")


app.layout = html.Div(children=[
    html.H1(children='Velib station map',
            style={
            'font_family': 'cursive',
            'textAlign': 'center'
        }),
    html.Div(children='The amount of velib not parked',
            style={
            'font_family': 'cursive',
            'textAlign': 'center'
        }),

    html.Div(children=velib_not_parked,
            style={
            'font_family': 'cursive',
            'textAlign': 'center'
        }),
    html.Div(children='Last update time',
            style={
            'font_family': 'cursive',
            'textAlign': 'center'
        }),

    html.Div(children=last_update,
            style={
            'font_family': 'cursive',
            'textAlign': 'center'
        }),
    dcc.Graph(figure=fig,style={"height": "800px", "width": "100%"} )
])


if __name__ == "__main__":
    app.run_server(host='0.0.0.0', port=8050, debug=True)