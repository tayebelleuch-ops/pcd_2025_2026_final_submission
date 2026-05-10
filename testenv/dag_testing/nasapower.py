LAT = 36.8      # Tunisia example
LON = 10.2
START = "19910101"
END = "20231231"

PARAMETERS = [
    "T2M",        # Temperature at 2m
    "PRECTOTCOR", # Corrected precipitation
    "RH2M",       # Relative humidity
    "WS2M"        ,# Wind speed
    "ALLSKY_SFC_SW_DWN",	#Downward shortwave solar radiation	W/m²
    "CLRSKY_SFC_SW_DWN"	  #Clear-sky solar radiation

]
import requests
import json

url = "https://power.larc.nasa.gov/api/temporal/daily/point"

params = {
    "latitude": LAT,
    "longitude": LON,
    "start": START,
    "end": END,
    "parameters": ",".join(PARAMETERS),
    "community": "AG",
    "format": "JSON"
}

r = requests.get(url, params=params, timeout=60)
r.raise_for_status()

data = r.json()