import requests
from dotenv import load_dotenv
import os

load_dotenv()
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

def get_twitch_access_token():

    headers = {
        "content-Type": "application/x-www-form-urlencoded"
    }

    token_url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(token_url, headers=headers, params=params)
    data = response.json()
    access_token = data['access_token']
    return access_token

# Contoh pemanggilan fungsi
access_token = get_twitch_access_token()
print('Access Token:', access_token)