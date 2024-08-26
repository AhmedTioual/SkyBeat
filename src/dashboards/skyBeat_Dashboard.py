import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
import logging
from time import sleep
import base64
import time
import plotly.express as px
import datetime

st.set_page_config(
        layout="wide",
        page_title="SkyBeat",
    )


def get_img_as_base64(file):
    with open(file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

img = get_img_as_base64("static/background.png")

css = f"""
    <style>
        body {{
        scroll-behavior: smooth;
        color: black;
        }}
        MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}

        [data-testid="stAppViewContainer"] > .main {{
            background-image: url("data:image/png;base64,{img}");
            background-size: cover;
            background-position: center;
            background-repeat: no-repeat;
            background-attachment: fixed;
            height: 100%;
        }}
        .st-emotion-cache-1jicfl2 {{
            height: 100%;
        }}
        .st-emotion-cache-1jicfl2 {{
            padding-top:0;
        }}
        h2 {{
            color:black;
        }}
        [data-testid="column"] {{
            background-color: #f7f7f7;
            padding: 30px;
            text-align: center;
            border-radius: 8px;
            box-shadow: rgba(0, 0, 0, 0.1) 0px 4px 12px;
            align-content: center;
        }}
        .st-emotion-cache-ubko3j svg {{
            display: none;
        }}

        [data-testid="stHorizontalBlock"] > st-emotion-cache-12w0qpk e1f1d6gn3 {{
            background:red;
        }}

        .st-emotion-cache-1085iqp svg {{
            stroke: rgba(0, 0, 0, 0.6);
            stroke-width: 2.25px;
            display: none;
        }}
        hr {{
            margin: 5px;
        }}
        .js-plotly-plot .plotly {{
            direction: ltr;
            font-family: "Open Sans", verdana, arial, sans-serif;
            margin: 0px;
            padding: 0px;
            box-shadow: rgba(0, 0, 0, 0.1) 0px 4px 12px;
        }}
        .main-svg {{
            border-radius : 8px;
        }}
        #skybeat-real-time-temperature-dashboard {{
            color:white;
        }}
    <style>
"""

st.markdown(css, unsafe_allow_html=True)

############# Start Cassandra Connection

def create_cassandra_connection():
    try:
        # Connect to the Cassandra cluster
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

session = create_cassandra_connection()

def fetch_data_from_cassandra(query):
    
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
    
############# End Cassandra Connection

st.title("SkyBeat | Real-Time Temperature Dashboard")
st.markdown("<br>", unsafe_allow_html=True)

# creating a single-element container.
placeholder = st.empty()

# Query to fetch data
query = "SELECT * FROM weather_data.weather_info;"

# Initialize an empty DataFrame to store all data
df_all = pd.DataFrame({})

# Streamlit plotly chart placeholder
plotly_chart = st.empty()

# Loop to fetch new data every 5 seconds
while True:
    
    data = fetch_data_from_cassandra(query)
    
    if not data.empty:
        
        # Convert 'dt' from Unix timestamp to datetime
        data['dt'] = pd.to_datetime(data['dt'])
        data['temp_celsius'] = data['temp_celsius'].astype(float).round(2)
        

        # Append new data to the existing DataFrame
        df_all = pd.concat([df_all, data]).drop_duplicates(subset='dt').sort_values(by='dt')
        
        # Calculate the average temperature
        average_temp = df_all['temp_celsius'].mean()

        latest_icon_url = df_all['weather_icon'].iloc[-1]
        latest_temp_celsius = df_all['temp_celsius'].iloc[-1]
        latest_Sunset = datetime.datetime.fromtimestamp(df_all['sys_sunset'].iloc[-1]).strftime('%H:%M')
        latest_sunrise = datetime.datetime.fromtimestamp(df_all['sys_sunrise'].iloc[-1]).strftime('%H:%M')
        latest_humidity = df_all['humidity'].iloc[-1]
        latest_Pressure = df_all['pressure'].iloc[-1]
        latest_humidity = df_all['humidity'].iloc[-1]
        latest_humidity = df_all['humidity'].iloc[-1]
        latest_wind_speed = df_all['wind_speed'].iloc[-1]
        latest_wind_deg = df_all['wind_deg'].iloc[-1]
        latest_Cloud = df_all['clouds_all'].iloc[-1]
        latest_visibility=df_all['visibility'].iloc[-1]

        with placeholder.container():
            # Create a two-column layout
            col1, col2 = st.columns([1, 2])  # Adjust the ratio as needed   

            with col1:
                st.markdown(f"<img src='{latest_icon_url}' alt='weather icon' style='margin-bottom: -15px;'>", unsafe_allow_html=True)
                st.markdown(f"<h1 style='color:#3D3C3C;font-size: 80px;'>{latest_temp_celsius}°C</h1>", unsafe_allow_html=True)
                st.markdown("<i style='color:#3D3C3C;font-size:20px'>Marrakesh, Morocco</i>", unsafe_allow_html=True)

            with col2:
                st.markdown('<h3>Weather Highlights</h3>', unsafe_allow_html=True)
                # Add vertical centering
                st.markdown(
                    """
                    <style>
                    .centered-content {
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        height: 100%;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True
                )

                # Define a function to create a card
                def create_card(title, value):
                    return f"""
                    <div style="text-align:center;">
                        <p style="font-size:24px; color:#666;">{title[0]}</p>
                        <h3 style="margin:0; color:#333;">{value[0]}</h3>
                        <hr>
                        <p style="font-size:24px; color:#666;">{title[1]}</p>
                        <h3 style="margin:0; color:#333;">{value[1]}</h3>
                    </div>
                    """

                # Center the cards vertically and horizontally in col2
                with col2:
                    st.markdown('<div class="centered-content">', unsafe_allow_html=True)
                    card1, card2, card3, card4 = st.columns(4)

                    with card1:
                        st.markdown(create_card(["Sunset 🌇","Sunrise 🌅"], [latest_Sunset,latest_sunrise]), unsafe_allow_html=True)
                    with card2:
                        st.markdown(create_card(["Humidity 💧","Pressure ⏲️"], [str(latest_humidity)+' %',str(latest_Pressure)+' hPa']), unsafe_allow_html=True)
                    with card3:
                        st.markdown(create_card(["Wind Speed ༄","Wind Degree 🚩"], [str(round(latest_wind_speed,2))+' m/s',str(latest_wind_deg)+'°']), unsafe_allow_html=True)
                    with card4:
                        st.markdown(create_card(["Cloud ☁️","Visibility 👁️"], [str(latest_Cloud)+' %',str(latest_visibility)+' meters']), unsafe_allow_html=True)
                    st.markdown('</div>', unsafe_allow_html=True)

        # Create a Plotly Express line chart with the updated data
        fig = px.line(df_all, x='dt', y='temp_celsius', text='temp_celsius', color_discrete_sequence=['red'])

        #fig.update_traces(texttemplate='<b>%{text}</b>', textposition='top right')
        
        # Add annotations for each bar
        fig.update_traces(
            hovertemplate='<b>Date Time :</b> %{x}<br><b>Temperature :</b> %{y} °C<extra></extra>'  # Custom hover labels
        )

        # Add a horizontal line for the average temperature
        fig.add_shape(
            type='line',
            x0=df_all['dt'].min(), x1=df_all['dt'].max(),
            y0=average_temp, y1=average_temp,
            line=dict(color='blue', width=2, dash='dash'),
            name='Average Temperature'
        )

        # Add a label for the average temperature line
        fig.add_annotation(
            x=df_all['dt'].max(), 
            y=average_temp, 
            text=f'Avg Temp: {average_temp:.2f}°C',
            showarrow=False,
            yshift=10,
            font=dict(color='blue')
        )

        fig.update_layout(
            plot_bgcolor='#f7f7f7',  # Background color of the plot area
            paper_bgcolor='#f7f7f7'      # Background color of the entire chart
        )

        # Remove x and y axis titles
        fig.update_xaxes(title_text='')
        fig.update_yaxes(title_text='')
        
        # Update the Streamlit plotly chart with the new figure
        plotly_chart.plotly_chart(fig, use_container_width=True)

    else:
        st.error("No data available or error fetching data")

    time.sleep(2)  # Wait for 2 seconds before fetching new data