import requests
import json


# ✅ Your API Key
API_KEY = 'f55544b68e9a2701c92c8515adaf6b7e'

# ✅ API Endpoint URL
url = f'http://api.aviationstack.com/v1/flights?access_key={API_KEY}'

# ✅ Make the API call
response = requests.get(url)

# ✅ Check if request was successful
if response.status_code == 200:
    data = response.json()  # Convert response to JSON format

    # ✅ Print first 5 flight records
    print("✅ API call successful. Printing first 5 flights:\n")
    for i, flight in enumerate(data['data'][:10], start=1):
        print(f"Flight {i}:")
        print(f"  Airline         : {flight['airline']['name']}")
        print(f"  Flight Number   : {flight['flight']['iata']}")
        print(f"  Departure       : {flight['departure']['airport']} at {flight['departure']['scheduled']}")
        print(f"  Arrival         : {flight['arrival']['airport']} at {flight['arrival']['scheduled']}")
        print(f"  Flight Status   : {flight['flight_status']}")
        print("-" * 40)


    # ✅ Save full data to a local JSON file
    with open("../Data/flights.json", "w") as f:
        json.dump(data, f, indent=4)

    print("\n📁 Full API response saved to 'Data/flights.json'")

else:
    print(f"❌ API call failed with status code: {response.status_code}")
    print("📌 Check your API key or internet connection.")
