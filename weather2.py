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
for keys,values in weather_data.items():
    print(keys)
    print(values)