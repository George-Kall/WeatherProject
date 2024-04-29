# Create a dictionary with the coordinates of 5 locations:
locations_dict = {
    'Thessaloniki, GR': {'lat': '40.6403', 'lon': '22.9439'},
    'Paris, FR': {'lat': '48.85341', 'lon': '2.3488'},
    'London, GB': {'lat': '51.50853', 'lon': '-0.12574'},
    'Dubai, AE': {'lat': '25.276987', 'lon': '55.296249'},
    'Los Angeles, US': {'lat': '34.0522', 'lon': '-118.2437'},
}
import requests
import pandas as pd
import api as apifile

api_key=apifile.api_key

def fetch_api_data(url: str) -> dict:
  """Fetches data from the specified API URL and returns the JSON response.

  Args:
      url: The URL of the API endpoint.

  Returns:
      The JSON data parsed from the API response, or raises an exception on error.

  Raises:
      requests.exceptions.HTTPError: If the API request fails.
  """

  response = requests.get(url)
  response.raise_for_status()  # Raise exception for non-200 status codes

  return response.json()



def get_weather_data(locations:dict, api_key: str) -> pd.DataFrame:
  """ Fetches current and forecasted weather data for multiple locations.

  Args:
      locations (dict): A dictionary containing location information.
          Each key represents a location name, and the value is another dictionary
          with 'lat' and 'lon' keys for latitude and longitude.
      api_key (str): Your OpenWeatherMap API key.

  Returns:
      tuple: A tuple containing two dictionaries, one for current weather data
          keyed by location name, and another for forecasted weather data
          keyed by location name.

  Raises:
      requests.exceptions.RequestException: If an error occurs during API requests.
  """

  base_url = "https://api.openweathermap.org/data/2.5/"
  weather_data = {"current": {}, "forecast": {}}

  for location_name, location in locations.items():
    try:
      # Construct URLs with common base and location data
      current_url = f"{base_url}weather?lat={location['lat']}&lon={location['lon']}&appid={api_key}&units=metric"
      forecast_url = f"{base_url}forecast?lat={location['lat']}&lon={location['lon']}&appid={api_key}&units=metric"

      # Fetch and store data using a single function call
      weather_data["current"][location_name] = fetch_api_data(current_url)
      weather_data["forecast"][location_name] = fetch_api_data(forecast_url)
    except requests.exceptions.RequestException as e:
      print(f"Error fetching data for location {location_name}: {e}")

  return weather_data

weather_data = get_weather_data(locations_dict, api_key)

import pandas as pd

def transform_current_weather_data(data_dict: dict) -> pd.DataFrame:
  """Transforms weather API data into a Pandas DataFrame suitable for BigQuery.

  Args:
      data_dict (dict): A dictionary containing weather data.

  Returns:
      pd.DataFrame: A Pandas DataFrame containing the transformed weather data.
  """

  # Preprocess the value of 'weather' key (in some cases the API returns a list instead of a dictionary)
  if isinstance(data_dict['weather'], list):  # Check if 'weather' is a list and select the first element
    data_dict['weather'] = data_dict['weather'][0]

  # Flatten data structure
  flattened_data = {}
  for key, value in data_dict.items():
    if isinstance(value, dict):
      for sub_key, sub_value in value.items():
        flattened_data[f"{key}_{sub_key}"] = sub_value
    else:
      flattened_data[key] = value

  # Convert DataFrame and handle datetime if necessary
  data_df = pd.DataFrame([flattened_data])
  if 'dt' in data_df.columns:
    data_df['dt_txt'] = pd.to_datetime(data_df['dt'], unit='s')
    data_df['dt_txt'] = data_df['dt_txt'].dt.strftime('%Y-%m-%d %H:%M:%S')

  return data_df


current_weather = pd.DataFrame()
for key, value in weather_data['current'].items():
    current_weather = pd.concat([current_weather, transform_current_weather_data(value)])


# Flattening forecast weather data
import pandas as pd
def convert_weather_api_dict_to_dataframe(data_dict: dict) -> pd.DataFrame:
  """
  Converts a nested dictionary containing weather data from the Weather API to a Pandas DataFrame.

  Args:
      data_dict (dict): The dictionary containing the weather data.

  Returns:
      pd.DataFrame: A DataFrame representing the weather data.
  """

  extracted_data = {}
  for key, value in data_dict.items():
    if isinstance(value, dict):
      for sub_key, sub_value in value.items():
        extracted_data[f"{key}_{sub_key}"] = sub_value
    else:
      extracted_data[key] = value

  return pd.DataFrame([extracted_data])


def transform_forecasted_weather_data(data_dict: dict) -> pd.DataFrame:
  """
  Transforms the forecasted weather data from the Weather API into a Pandas DataFrame.

  Args:
      data_dict (dict): The dictionary containing the forecasted weather data.

  Returns:
      pd.DataFrame: A DataFrame containing the transformed forecasted weather data.
  """

  city_dict = data_dict['city']
  city_df = convert_weather_api_dict_to_dataframe(city_dict)

  forecasts_dict = data_dict['list']
  forecast_df = pd.DataFrame()
  for forecast_item in forecasts_dict:
    forecast_item['weather'] = forecast_item['weather'][0]
    forecast_item_df = convert_weather_api_dict_to_dataframe(forecast_item)
    forecast_df = pd.concat([forecast_df, forecast_item_df], ignore_index=True)

  return pd.concat([forecast_df, city_df], axis=1)

forecast_weather = pd.DataFrame()
for key, value in weather_data['forecast'].items():
    forecast_weather = pd.concat([forecast_weather, transform_forecasted_weather_data(value)])