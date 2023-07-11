from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import time
import psycopg2
import numpy as np
import seaborn as sns
import dash_bootstrap_components as dbc

db_host = "postgres"
db_port = 5432
db_user = "airflow"
db_password = "airflow"
db_name = "velib"

# Function to create the database if it does not exist
def create_database():
    # Connect to the default PostgreSQL database (template1)
    conn = psycopg2.connect(host=db_host, port=db_port, user=db_user, password=db_password)
    conn.autocommit = True
    cursor = conn.cursor()

    # Check if the database already exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s;", (db_name,))
    database_exists = cursor.fetchone()

    if not database_exists:
        # Create the database
        cursor.execute(f"CREATE DATABASE {db_name};")
        print(f"Database '{db_name}' created.")

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Function to create the tables if they do not exist and insert initial data
def create_tables():
    # Connect to the target database
    conn = psycopg2.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=db_name)
    conn.autocommit = True
    cursor = conn.cursor()

    # Check if the tables already exist
    cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'velib_station_infos')")
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # Create the velib_station_infos table
        cursor.execute("""
            CREATE TABLE velib_station_infos (
                name VARCHAR(100),
                lat FLOAT(10),
                lon FLOAT(10),
                capacity INT,
                stationCode VARCHAR(20)
            );
        """)

        # Insert initial data into velib_station_infos table
        cursor.execute("""
            INSERT INTO velib_station_infos (name, lat, lon, capacity, stationCode)
            VALUES ('Station 1', 48.8584, 2.2945, 20, 'ABC123');
        """)

        print("Table 'velib_station_infos' created and initial data inserted.")

    # Check if the tables already exist
    cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'velib_station_status')")
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # Create the velib_station_status table
        cursor.execute("""
            CREATE TABLE velib_station_status (
                stationCode VARCHAR(20),
                numBikesAvailable SMALLINT,
                numDocksAvailable SMALLINT,
                is_installed SMALLINT,
                is_returning SMALLINT,
                is_renting SMALLINT,
                last_reported INT
            );
        """)

        # Insert initial data into velib_station_status table
        cursor.execute("""
            INSERT INTO velib_station_status (stationCode, numBikesAvailable, numDocksAvailable, is_installed, is_returning, is_renting, last_reported)
            VALUES ('ABC123', 10, 10, 1, 1, 1, 1626036000);
        """)

        print("Table 'velib_station_status' created and initial data inserted.")

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Function to fetch data from the database
def fetch_data():
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
    cursor = conn.cursor()

    # Fetch data from the tables
    cursor.execute("SELECT * FROM velib_station_infos")
    data_infos = cursor.fetchall()

    

    # Create dataframes from the fetched data
    cols_infos = [elt[0] for elt in cursor.description]
    dataframe_with_station_informations = pd.DataFrame(data=data_infos, columns=cols_infos)

    cursor.execute("SELECT * FROM velib_station_status")
    data_status = cursor.fetchall()

    cols_status = [elt[0] for elt in cursor.description]
    dataframe_with_station_status = pd.DataFrame(data=data_status, columns=cols_status)

    # Merge the dataframes
    df = pd.merge(dataframe_with_station_status, dataframe_with_station_informations, on=["stationcode"])

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return df


# Create the database and tables if they do not exist
create_database()
create_tables()

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
