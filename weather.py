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
api_key = '7f59c14120fb0695f9ccdf693d149754' 
 
lat = '48.85341' # Use the latitude of your desired location
lon = '2.3488' # Use the longitude of your desired location

# for current weather
url = 'https://api.openweathermap.org/data/2.5/weather'

# for weathwer forecast days every 3 hours
url = "http://api.openweathermap.org/data/2.5/forecast" 
complete_url = f"{url}?lat={lat}&lon={lon}&appid={api_key}&units=metric"

response = requests.get(complete_url)
    
# Read response:
if response.status_code == 200:
    response_json = response.json()
    print(response_json)
else:
    print(f"Error: Unable to fetch data. Status code {response.status_code}")
    raise requests.exceptions.HTTPError(response.text)

