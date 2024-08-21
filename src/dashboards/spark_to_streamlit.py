import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType
import streamlit as st

# Define schema for the weather data
def get_weather_schema():
    return StructType([
        StructField("coord", StructType([
            StructField("lon", FloatType(), False),
            StructField("lat", FloatType(), False)
        ]), False),
        StructField("weather", ArrayType(StructType([
            StructField("id", IntegerType(), False),
            StructField("main", StringType(), False),
            StructField("description", StringType(), False),
            StructField("icon", StringType(), False)
        ])), False),
        StructField("base", StringType(), False),
        StructField("main", StructType([
            StructField("temp", FloatType(), False),
            StructField("feels_like", FloatType(), False),
            StructField("temp_min", FloatType(), False),
            StructField("temp_max", FloatType(), False),
            StructField("pressure", IntegerType(), False),
            StructField("humidity", IntegerType(), False),
            StructField("sea_level", IntegerType(), False),
            StructField("grnd_level", IntegerType(), False)
        ]), False),
        StructField("visibility", IntegerType(), False),
        StructField("wind", StructType([
            StructField("speed", FloatType(), False),
            StructField("deg", IntegerType(), False)
        ]), False),
        StructField("clouds", StructType([
            StructField("all", IntegerType(), False)
        ]), False),
        StructField("dt", IntegerType(), False),
        StructField("sys", StructType([
            StructField("type", IntegerType(), False),
            StructField("id", IntegerType(), False),
            StructField("country", StringType(), False),
            StructField("sunrise", IntegerType(), False),
            StructField("sunset", IntegerType(), False)
        ]), False),
        StructField("timezone", IntegerType(), False),
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("cod", IntegerType(), False),
        StructField("temp_celsius", FloatType(), False)
    ])

# Create and configure a SparkSession to connect to Spark and Kafka.
def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('StreamlitRealTimeDashboard') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")\
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")
    return s_conn

# Connect Spark to a Kafka topic to read streaming data.
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'weather_topic') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
    return spark_df

# Process the raw Kafka data and apply a schema to it.
def create_selection_df_from_kafka(spark_df):
    if spark_df is None:
        raise ValueError("spark_df is None. Check Kafka stream source.")
    
    schema = get_weather_schema()

    # Convert the Kafka value to a string and parse using the defined schema
    raw_df = spark_df.selectExpr("CAST(value AS STRING)")

    # Extract nested fields and handle arrays
    flattened_df = raw_df.select(from_json(col('value'), schema).alias('data')) \
        .select(
            col('data.coord.lon').alias('coord_lon'),
            col('data.coord.lat').alias('coord_lat'),
            col('data.weather').alias('weather'),
            col('data.base'),
            col('data.main.temp').alias('temp'),
            col('data.main.feels_like').alias('feels_like'),
            col('data.main.temp_min').alias('temp_min'),
            col('data.main.temp_max').alias('temp_max'),
            col('data.main.pressure').alias('pressure'),
            col('data.main.humidity').alias('humidity'),
            col('data.main.sea_level').alias('sea_level'),
            col('data.main.grnd_level').alias('grnd_level'),
            col('data.visibility'),
            col('data.wind.speed').alias('wind_speed'),
            col('data.wind.deg').alias('wind_deg'),
            col('data.clouds.all').alias('clouds_all'),
            col('data.dt'),
            col('data.sys.type').alias('sys_type'),
            col('data.sys.id').alias('sys_id'),
            col('data.sys.country').alias('sys_country'),
            col('data.sys.sunrise').alias('sys_sunrise'),
            col('data.sys.sunset').alias('sys_sunset'),
            col('data.timezone'),
            col('data.id'),
            col('data.name'),
            col('data.cod'),
            col('data.temp_celsius')
        )

    # Handle the 'weather' array by exploding it
    weather_df = flattened_df.withColumn('weather', explode(col('weather')))

    # Flatten the exploded weather array
    final_df = weather_df.select(
        col('coord_lon'),
        col('coord_lat'),
        col('weather.id').alias('weather_id'),
        col('weather.main').alias('weather_main'),
        col('weather.description').alias('weather_description'),
        col('weather.icon').alias('weather_icon'),
        col('base'),
        col('temp'),
        col('feels_like'),
        col('temp_min'),
        col('temp_max'),
        col('pressure'),
        col('humidity'),
        col('sea_level'),
        col('grnd_level'),
        col('visibility'),
        col('wind_speed'),
        col('wind_deg'),
        col('clouds_all'),
        col('dt'),
        col('sys_type'),
        col('sys_id'),
        col('sys_country'),
        col('sys_sunrise'),
        col('sys_sunset'),
        col('timezone'),
        col('id'),
        col('name'),
        col('cod'),
        col('temp_celsius')
    )
    
    # Print the schema for debugging
    print(final_df.printSchema())
    
    return final_df

# Function to fetch data from the in-memory table
#@st.cache_data(ttl=5)
def get_data(spark=None):
    print(spark.sql("SHOW TABLES").show())
    spark.sql("SELECT * FROM weather_info").show()

# Streamlit app code
def run_streamlit_dashboard(spark=None):
    st.title("Real-Time Weather Dashboard")
    st.subheader("Weather Data for Marrakech")

    # Fetch and display data
    data = get_data(spark)
    if not data.empty:
        st.write(data)
        st.line_chart(data[["temp_celsius", "dt"]])
    else:
        st.write("No data available yet.")

# Streaming logic
def start_streaming(spark=None):
    
    kafka_df = connect_to_kafka(spark)
    selection_df = create_selection_df_from_kafka(kafka_df)
    
    transformed_df = selection_df.withColumn("temp_celsius", col("temp") - 273.15)  # Convert Kelvin to Celsius
    
    streaming_query = (transformed_df.writeStream
                       .outputMode("append")
                       .format("memory")
                       .queryName("weather_info")
                       .start())
    
    logging.info("Streaming has started...")
    return streaming_query

if __name__ == "__main__":
    
    # Create spark connection
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        
        # Connect to Kafka with spark connection
        streaming = start_streaming(spark_conn)
        
        # Run the Streamlit dashboard
        #run_streamlit_dashboard(spark_conn)
        print(get_data(spark_conn))
        # Await termination of the streaming query
        streaming.awaitTermination()    