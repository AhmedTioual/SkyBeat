import requests
import json
from kafka import KafkaProducer
import time
import logging
import random
from datetime import datetime

city_name = "Marrakech"
api_key = "6bc10cad84f55de2373784c834d75633"

def get_weather(api_key, city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url).json()
    return response

# Configure logging
logging.basicConfig(level=logging.INFO)

def stream_data(
    topic: str = 'weather_topic',
    bootstrap_servers: list = ['0.0.0.0:9092'],
    max_block_ms: int = 5000,
    retry_attempts: int = 3,
    retry_delay: int = 5,
    get_weather: callable = get_weather,  # Function to get weather data
    city_name: str = 'Marrakesh',
    api_key: str = api_key
) -> None:
    """
    Streams weather data to a Kafka topic indefinitely with slight temperature changes.

    Parameters:
    - topic: Kafka topic to send data to.
    - bootstrap_servers: List of Kafka broker addresses.
    - max_block_ms: Maximum time to block waiting for space in the buffer.
    - retry_attempts: Number of times to retry on failure.
    - retry_delay: Delay between retries in seconds.
    - get_weather: Function to get weather data.
    - city_name: City name for the weather query.
    - api_key: API key for authentication.
    """
    if get_weather is None:
        raise ValueError("A 'get_weather' function must be provided.")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        max_block_ms=max_block_ms
    )
    
    # Initialize previous temperature for simulation
    previous_temp = None
    
    while True:  # Infinite loop to continuously send data
        attempt = 0
        while attempt < retry_attempts:
            try:
                result = get_weather(api_key, city_name)
                
                # Modify the 'dt' field to the current timestamp
                result['dt'] = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                
                result['weather'][0]['icon'] = f"https://openweathermap.org/img/wn/{result['weather'][0]['icon']}@2x.png"
                
                # Update the temperature
                current_temp = result['main']['temp']
                
                if previous_temp is not None:
                    result['main']['temp'] = previous_temp + random.uniform(-0.5, 0.5)
                    result['main']['humidity'] = int(result['main']['humidity'] + random.uniform(-0.5, 0.5))
                    result['main']['pressure'] = int(result['main']['pressure'] + random.uniform(-0.5, 0.5))
                    result['wind']['speed'] = result['wind']['speed'] + random.uniform(-0.5, 0.5)
                    result['wind']['deg'] = int(result['wind']['deg'] + random.uniform(-0.5, 0.5))
                    result['clouds']['all'] = int(result['clouds']['all'] + random.uniform(-0.2, 0.2))
                    result['visibility'] = int(result['visibility'] + random.uniform(-0.2, 0.2))
                else:
                    # Initialize previous_temp if it's the first iteration
                    previous_temp = current_temp
                
                # Update previous temperature
                previous_temp = result['main']['temp']
                
                producer.send(topic, json.dumps(result).encode('utf-8'))
                logging.info(f"Data sent to topic '{topic}': {result}")
                break  # Exit retry loop on success
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} failed: {e}")
                attempt += 1
                time.sleep(retry_delay)
        
        if attempt == retry_attempts:
            logging.error("Max retry attempts reached. Skipping this iteration.")
        
        # Wait 5 seconds before sending the next data
        time.sleep(2)
    
    producer.close()

if __name__ == "__main__":
    stream_data()