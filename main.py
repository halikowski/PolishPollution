import pandas as pd
import requests
import json
import boto3
import os
import datetime
import pytz
import logging

logging.basicConfig(level=logging.DEBUG, encoding='utf-8')

# Get AWS credentials
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')

timezone = pytz.timezone('Europe/Warsaw')
CURRENT_TIMESTAMP = datetime.datetime.now(timezone).strftime('%Y-%m-%dT%H:%M')

# Convert csv file to JSON
cities = (pd.read_csv("./PLcities_over_100k.csv", delimiter=';').
               to_json(orient='records'))

# Convert JSON to iterable array
cities_data = json.loads(cities)

# Extract coordinates - as latitudes and longitudes separately, for API calling on each pair
lats = [city['Latitude'] for city in cities_data]
lons = [city['Longitude'] for city in cities_data]

# Empty arrays to gather all API outputs
aq_data = []
weather_data = []

def call_openweather_api(endpoint,res_array,coords):
    """
    :param endpoint:
    :param res_array:
    :param coords:
    :return:

    Takes API endpoint, outer array name and pairs of latitude & longitude to fetch air quality or
    weather data from the OpenWeatherMap API. Returns json_file ready for further upload.
    """
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    for lat,lon in coords:
        if endpoint == 'weather':
            url = f'http://api.openweathermap.org/data/2.5/{endpoint}?units=metric&lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}'
        else:
            url = f'http://api.openweathermap.org/data/2.5/{endpoint}?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}'
        try:
            response = requests.get(url, headers=headers)
            data = response.json()
            # swap response's coords with the original input coords from .csv file
            data['coord']['lat'] = lat
            data['coord']['lon'] = lon
            res_array.append(data)
            logging.info('Successfully fetched api data')
        except Exception as e:
            logging.info(f'An error occured while calling the API: {e}')
    wrapped_array = {"results": res_array}
    json_file = json.dumps(wrapped_array, ensure_ascii=False, indent=4).encode('utf-8')

    return json_file


aq_file = call_openweather_api('air_pollution',aq_data,zip(lats,lons))
weather_file = call_openweather_api('weather',weather_data,zip(lats,lons))

def send_to_s3(file_name,file_body, directory):
    """
    :param file_name:
    :param file_body:
    :param directory:
    :return:

    Takes file name and body to create new file in AWS S3 bucket, at specified directory. Puts additional timestamp
    into file name for distinct naming after each API call(thus, upload).
    """
    global CURRENT_TIMESTAMP
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    bucket_name = 'mateuszairqualitydata'
    try:
        s3.put_object(Bucket=bucket_name, Key=f'raw/{directory}/{CURRENT_TIMESTAMP}_{file_name}', Body=file_body)
        logging.info(f"Data successfully uploaded to s3://{bucket_name}/raw/{directory}/{CURRENT_TIMESTAMP}_{file_name}")
    except Exception as e:
        logging.info(f"Failed to upload data: {e}")


send_to_s3('aq_data.json', aq_file,'pollution')
send_to_s3('weather_data.json', weather_file, 'weather')
