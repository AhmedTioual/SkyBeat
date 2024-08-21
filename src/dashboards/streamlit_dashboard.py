import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
import logging
from time import sleep

def create_cassandra_connection():
    try:
        # Connect to the Cassandra cluster
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def fetch_data_from_cassandra(query):
    session = create_cassandra_connection()
    if session:
        try:
            rows = session.execute(query)
            # Convert rows to DataFrame
            df = pd.DataFrame(list(rows))
            return df
        except Exception as e:
            logging.error(f"Could not fetch data from Cassandra due to {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of error
    else:
        return pd.DataFrame()  # Return an empty DataFrame if connection fails
    
def main():
    st.title("Real-Time Temperature Dashboard")

    query = "SELECT dt, temp_celsius FROM weather_data.weather_info;"  # Replace with your actual query
    placeholder = st.empty()  # Create a placeholder for the chart

    # Initialize the chart placeholder
    chart_placeholder = st.empty()

    # Initialize an empty DataFrame to store data
    df_all = pd.DataFrame()

    while True:
        data = fetch_data_from_cassandra(query)
        if not data.empty:
            # Convert 'dt' from Unix timestamp to datetime
            data['dt'] = pd.to_datetime(data['dt'], unit='s')
            data['temp_celsius'] = data['temp_celsius'].astype(float)
            
            # Append new data to the existing DataFrame
            df_all = pd.concat([df_all, data]).drop_duplicates(subset='dt')
            
            # Update the line chart in the placeholder
            chart_placeholder.line_chart(df_all.set_index('dt')['temp_celsius'])
        else:
            placeholder.write("No data available or error fetching data")

        sleep(5)  # Wait for 5 seconds before fetching new data

if __name__ == "__main__":
    main()
