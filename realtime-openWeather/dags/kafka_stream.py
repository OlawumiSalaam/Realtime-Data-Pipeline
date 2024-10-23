from datetime import datetime, timedelta, timezone
import time                           
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import logging
from kafka import KafkaProducer  

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    'owner': '3Signet',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 15),
    'email': ['olawumisalaam@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

city_name = "Lagos"
country = "NG"
base_url = "https://api.openweathermap.org/data/2.5/forecast?q=Lagos,NG&appid="

# Load API key
with open("/opt/airflow/credentials.txt", 'r') as f:
    api_key = f.read()
full_url = base_url + api_key

def get_data(full_url):
    """Fetch weather data from OpenWeather API."""
    logging.info("Fetching data from API...")
    try:
        r = requests.get(full_url)
        r.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        data = r.json()
        logging.info("Data fetched successfully.")
        return data
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except Exception as err:
        logging.error(f"An error occurred: {err}")

def format_data(data):
    """Process and return detailed weather forecast data in a dictionary format, including timezone-aware times.
       This function also performs initial data cleaning, such as handling missing values and ensuring consistent formats."""
    processed_data = {
        'city': {},
        'forecasts': []
    }

    if data:
        # Get the timezone offset from the response (in seconds)
        city_timezone_offset = data['city'].get('timezone', 0)  # Shift in seconds from UTC
        city_timezone = timezone(timedelta(seconds=city_timezone_offset))

        # Extract general information about the city
        processed_data['city'] = {
            'name': data['city']['name'],
            'country': data['city']['country'],
            'timezone_offset': city_timezone_offset,
            'sunrise': datetime.fromtimestamp(data['city']['sunrise'], tz=timezone.utc).astimezone(city_timezone).strftime('%Y-%m-%d %H:%M:%S %Z'),
            'sunset': datetime.fromtimestamp(data['city']['sunset'], tz=timezone.utc).astimezone(city_timezone).strftime('%Y-%m-%d %H:%M:%S %Z')
        }

        # Process each forecast in the list
        for forecast in data['list']:
            # Convert forecast timestamp to local city time
            forecast_time = datetime.fromtimestamp(forecast['dt'], tz=timezone.utc).astimezone(city_timezone)

            # Handle missing values and clean the data
            forecast_data = {
                'timestamp_local': forecast_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'temperature': forecast['main'].get('temp', 0),  # Default to 0 if missing
                'feels_like': forecast['main'].get('feels_like', 0),
                'temp_min': forecast['main'].get('temp_min', 0),
                'temp_max': forecast['main'].get('temp_max', 0),
                'pressure': forecast['main'].get('pressure', 'N/A'),
                'humidity': forecast['main'].get('humidity', 'N/A'),
                'weather': {
                    'description': forecast['weather'][0].get('description', 'No description available'),
                    'main': forecast['weather'][0].get('main', 'Unknown'),
                    'id': forecast['weather'][0].get('id', 'N/A'),
                    'icon': forecast['weather'][0].get('icon', 'N/A')
                },
                'cloudiness': forecast['clouds'].get('all', 0),
                'wind': {
                    'speed': forecast['wind'].get('speed', 0),
                    'direction': forecast['wind'].get('deg', 'N/A'),
                    'gust': forecast['wind'].get('gust', 'N/A')  # Handle missing gust data
                },
                'visibility': forecast.get('visibility', 'N/A'),  # Handle missing visibility
                'precipitation_probability': forecast.get('pop', 'N/A'),
                'rain_volume_last_3h': forecast.get('rain', {}).get('3h', 0),  # Default to 0 if rain data is missing
                'snow_volume_last_3h': forecast.get('snow', {}).get('3h', 0),  # Default to 0 if snow data is missing
                'part_of_day': 'Day' if forecast['sys'].get('pod', 'n') == 'd' else 'Night'
            }

            # Append each processed forecast to the 'forecasts' list
            processed_data['forecasts'].append(forecast_data)

        # Optional: Check for duplicates by timestamp and remove them
        unique_forecasts = {forecast['timestamp_local']: forecast for forecast in processed_data['forecasts']}
        processed_data['forecasts'] = list(unique_forecasts.values())

    else:
        logging.warning("No data to process.")
    
    return processed_data

import time
import logging
import json
from kafka import KafkaProducer

def stream_data():
    """Stream formatted weather data to Kafka."""
    logging.info("Starting data stream...")

    try:
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    except Exception as e:
        logging.error(f"Failed to create Kafka producer: {e}")
        return
    
    curr_time = time.time()
    backoff_time = 10  # Start with 10 seconds
    max_backoff = 300  # Maximum backoff time of 5 minutes (300 seconds)

    while True:
        if time.time() > curr_time + 60:  # 1 minute limit for streaming
            logging.info("Stopping data stream after 1 minute.")
            break

        try:
            data = get_data(full_url)  # Ensure `full_url` is correctly defined elsewhere
            if data:
                formatted_data = format_data(data)
                logging.info("Data formatted successfully.")
                
                # Sending data to Kafka
                try:
                    producer.send('3Signet_weatherData', json.dumps(formatted_data).encode('utf-8'))
                    logging.info("Data sent to Kafka topic '3Signet_weatherData'.")
                    
                    # Reset the backoff time since the request was successful
                    backoff_time = 10
                    
                except Exception as e:
                    logging.error(f"Failed to send data to Kafka: {e}")
            else:
                logging.warning("No data received from API.")

        except Exception as e:
            logging.error(f"Error fetching or processing data: {e}")
            logging.info(f"Retrying in {backoff_time} seconds...")
            
            # Wait for the current backoff time before retrying
            time.sleep(backoff_time)
            
            # Double the backoff time, but cap it at the max_backoff value
            backoff_time = min(backoff_time * 2, max_backoff)
            continue

    producer.close()  # Ensure the producer is closed after the loop ends





with DAG('weather_dag_automation',
         default_args=default_args,
         schedule_interval=timedelta(hours=3),
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )



    




    
