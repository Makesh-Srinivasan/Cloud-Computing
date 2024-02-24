import requests
import json

API_KEY = "dlthaLMuMtGxh09Q_YE2EYQG_gRWVsVSPTlRHhVNEMw5lsmZgcwfLFFnCuqlW1FeJt60_r1JfjP9zWLWDdVsXEQbJ8Q3v3jEqBG2jfjTaMzEL2UWlWDqptOC_sDWZXYx"
API_URL_ENDPOINT = "https://api.yelp.com/v3/businesses/search"
API_LIMIT = 50
HEADERS = {"Authorization": "Bearer " + API_KEY}


def fetch_data_yelp(cuisine, MAX_RECORDS):
    with open(cuisine+'.json', 'w') as writer:
        records = []
        for c in range(0, MAX_RECORDS, API_LIMIT):
            params = {"location": "New York City", "term": cuisine, "categories": "Restaurants", "limit": API_LIMIT,"offset": c}
            response = requests.get(API_URL_ENDPOINT, params=params, headers=HEADERS).json()
            for rec in response['businesses']:
                records.append(rec)
        json.dump(records, writer)


if __name__ == "__main__":
    CUISINES = ['Indian', 'Chinese', 'Japanese']
    for cuisine in CUISINES:
        fetch_data_yelp(cuisine, 50)