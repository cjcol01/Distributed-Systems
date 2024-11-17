import azure.functions as func
import logging
import requests
import os
import json
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from azure.storage.blob import BlobServiceClient
import azure.core.pipeline.policies as policies


policies.HttpLoggingPolicy.DEFAULT_HEADERS_WHITELIST = set()
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
logging.getLogger('azure').setLevel(logging.WARNING)
logging.getLogger('azure.storage.blob').setLevel(logging.WARNING)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# get weather data every hour and store it
@app.schedule(schedule="0 0 * * * *", arg_name="timer", run_on_startup=True)
def fetch_and_store_weather(timer: func.TimerRequest) -> None:
    logging.info('starting hourly weather data collection')
    
    try:
        # local/ POI coords
        locations = [
            {"name": "Leeds", "lat": 53.8008, "lon": -1.5491},
            {"name": "London", "lat": 51.5074, "lon": -0.1278},
        ]
        
        all_weather_data = []
        
        for location in locations:
            # need lat,lon format for azure maps api
            coord_query = f"{location['lat']:.6f},{location['lon']:.6f}"
            
            key = os.environ["AZURE_MAPS_KEY"]
            url = "https://atlas.microsoft.com/weather/forecast/daily/json"
            params = {
                'api-version': '1.1',
                'query': coord_query,
                'duration': 1,  # 1 day forecast
                'subscription-key': key
            }

            logging.info(f'getting weather for {location["name"]}')
            response = requests.get(url, params=params)
            
            if response.status_code != 200:
                logging.error(f'api error for {location["name"]}: {response.status_code} - {response.text}')
                continue
                
            raw_data = response.json()
            location_data = format_weather_data(raw_data, location["name"])
            all_weather_data.append(location_data)
            
            # save city data
            store_weather_data(location_data, location["name"])
        
        # save combined data for email
        store_weather_data({'locations': all_weather_data}, 'combined')
        logging.info('weather data updated')
        
    except Exception as e:
        logging.error(f'error getting weather: {str(e)}')

def format_weather_data(raw_data, location_name):
    # clean up the raw api data into something nice
    formatted_data = {
        'location': location_name,
        'forecasts': []
    }
    
    for day in raw_data['forecasts']:
        date = datetime.fromisoformat(day['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
        min_temp = day['temperature']['minimum']['value']
        max_temp = day['temperature']['maximum']['value']
        
        day_info = {}
        if 'day' in day:
            day_data = day['day']
            day_info = {
                'description': day_data['shortPhrase'],
                'rain_chance': day_data['precipitationProbability'],
                'feels_like': day_data.get('realFeelTemperature', {}).get('value', None)
            }
            
            # only include wind if it's significant
            if 'wind' in day_data:
                wind_speed = day_data['wind']['speed']['value']
                if wind_speed > 15:
                    day_info['wind'] = {
                        'speed': wind_speed,
                        'direction': day_data['wind']['direction']['localizedDescription']
                    }
        
        day_summary = {
            'date': date,
            'temp_range': f"{min_temp:.1f}°C to {max_temp:.1f}°C",
            'day': day_info
        }
        
        if 'night' in day:
            day_summary['night'] = {
                'description': day['night']['shortPhrase']
            }
        
        formatted_data['forecasts'].append(day_summary)
    
    return formatted_data

def store_weather_data(weather_data, location_name):
    # save to azure blob storage
    try:
        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if connection_string == "UseDevelopmentStorage=true":
            connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client("weather-history")
        
        # create container if it doesn't exist
        try:
            container_client.get_container_properties()
        except Exception:
            container_client.create_container()
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        blob_name = f"{location_name}/{current_date}.json"
        
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json.dumps(weather_data), overwrite=True)
        
        logging.info(f"saved weather data for {location_name}")
        return True
        
    except Exception as e:
        logging.error(f"failed to save data: {str(e)}")
        return False

def get_stored_weather_data():
    # get data from storage for emails
    try:
        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if connection_string == "UseDevelopmentStorage=true":
            connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client("weather-history")
        
        # check if container exists
        try:
            container_client.get_container_properties()
        except Exception as e:
            logging.error(f"Container doesn't exist: {str(e)}")
            return None
            
        current_date = datetime.now().strftime("%Y-%m-%d")
        blob_name = f"combined/{current_date}.json"
        
        # check if blob exists before trying to download
        blob_client = container_client.get_blob_client(blob_name)
        if not blob_client.exists():
            logging.error(f"Blob {blob_name} not found")
            return None
            
        # download and decode blob data
        downloaded_blob = blob_client.download_blob()
        blob_data = downloaded_blob.readall()
        if not blob_data:
            logging.error("Downloaded blob is empty")
            return None
            
        # cecode and parse JSON
        json_data = json.loads(blob_data.decode('utf-8'))
        return json_data
        
    except Exception as e:
        logging.error(f"couldn't get weather data: {str(e)}")
        return None

# send weather email at 7am
@app.schedule(schedule="0 0 7 * * *", arg_name="timer", run_on_startup=False)
# @app.schedule(schedule="*/5 * * * * *", arg_name="timer", run_on_startup=True)
def send_weather_email(timer: func.TimerRequest) -> None:
    logging.info('preparing daily weather email')
    
    try:
        weather_data = get_stored_weather_data()
        
        if weather_data and 'locations' in weather_data:
            email_body = create_email_content(weather_data['locations'])
            send_formatted_email(email_body)
            logging.info('\033[1;92memail sent successfully!\033[0m')
        else:
            raise Exception("no weather data for email")
            
    except Exception as e:
        logging.error(f'failed to send email: {str(e)}')

def create_email_content(weather_data):
    # make email with emojis and alerts
    email_body = "🌤️ Daily Weather Report 🌤️\n\n"
    
    for location_data in weather_data:
        email_body += f"\n📍 {location_data['location'].upper()}\n"
        email_body += "=" * 40 + "\n"
        
        for day in location_data['forecasts']:
            email_body += f"\n📅 {day['date']}\n"
            email_body += f"🌡️ Temperature: {day['temp_range']}\n"
            
            if 'day' in day:
                day_info = day['day']
                email_body += f"☀️ Day: {day_info['description']}\n"
                email_body += f"🌧️ Rain chance: {day_info['rain_chance']}%\n"
                
                if day_info['rain_chance'] > 70:
                    email_body += "⚠️ High chance of rain - bring an umbrella!\n"
                
                if 'feels_like' in day_info and day_info['feels_like']:
                    email_body += f"🌡️ Feels like: {day_info['feels_like']}°C\n"
                
                if 'wind' in day_info:
                    email_body += f"💨 Wind: {day_info['wind']['speed']} km/h {day_info['wind']['direction']}\n"
            
            if 'night' in day:
                email_body += f"🌙 Night: {day['night']['description']}\n"
            
            email_body += "-" * 40 + "\n"
    
    return email_body

def send_formatted_email(email_body):
    # send via gmail
    msg = MIMEMultipart()
    msg['From'] = os.environ["GMAIL_USER"]
    msg['To'] = os.environ["TO_EMAIL"]
    msg['Subject'] = '🌤️ Your Daily Weather Report'
    msg.attach(MIMEText(email_body, 'plain'))
    
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(os.environ["GMAIL_USER"], os.environ["GMAIL_APP_PASSWORD"])
        server.send_message(msg)