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

df["last_reported"] = pd.to_datetime(df['last_reported'],unit='s')

last_update = df["last_reported"].max().strftime("%H:%M %d/%m/%Y")

velib_not_parked = np.sum(df["capacity"]-df["numDocksAvailable"])

#print("Connection established to: ",data)

#Closing the connection
conn.close()

app = Dash(__name__,external_stylesheets=[dbc.themes.LUX])

fig = px.scatter_mapbox(df, lat="lat", lon="lon", color="numBikesAvailable",size="capacity",
                  color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=10)
fig.update_layout(mapbox_style="open-street-map")
# colors = {
#     'background': '#111111',
#     'text': '#7FDBFF'
# }

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
    dcc.Graph(figure=fig,style={"height": "1000px", "width": "100%"} )
])


#  dcc.Graph(
#         id='example-graph',
#         figure=fig
#     )
# @app.callback(
#     Output('my-output',component_property='children'),
#     Input('refresh-button')
# )

def update_table():
    ingestion_flow()
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    return current_time

if __name__ == "__main__":
    app.run_server(host='0.0.0.0', port=8050, debug=True)