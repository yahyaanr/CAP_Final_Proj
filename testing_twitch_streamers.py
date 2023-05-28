from dotenv import load_dotenv
import requests
import os
from datetime import datetime
import csv
from tqdm import tqdm
import time

load_dotenv()
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
access_token = os.getenv("ACCESS_TOKEN")
tsv_file_path = str(os.getenv("ENV_PATH"))

def save_streamer_to_csv(streamer, tsv_file_path):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    maturity = str(streamer['is_mature'])  # Convert boolean to string
    row = [
        timestamp,
        streamer['id'],
        streamer['user_id'],
        streamer['user_login'],
        streamer['user_name'],
        streamer['started_at'],
        streamer['title'],
        streamer['game_id'],
        streamer['game_name'],
        streamer['type'],
        streamer['language'],
        maturity,  # Use the converted value
        streamer['thumbnail_url'],
        str(streamer['viewer_count'])  # Convert integer to string
    ]

    with open(tsv_file_path, 'a', newline='') as file:
        writer = csv.writer(file, delimiter='\t')
        writer.writerow(row)


def get_twitch_streamers(access_token, first, tsv_file_path, save_interval=1000):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Client-Id': client_id
    }
    params = {
        'first': first,
    }
    url = 'https://api.twitch.tv/helix/streams'
    save_count = 0
    start_time = time.time()
    
    if not os.path.isfile(tsv_file_path):
        with open(tsv_file_path, 'w', newline='') as file:
            header = [
                'Timestamp',
                'StreamID',
                'StreamerUserID',
                'StreamerLoginName',
                'StreamerDisplayName',
                'StreamerStartTime',
                'StreamerTitle',
                'GameID',
                'GameName',
                'StreamType',
                'StreamLanguage',
                'Maturity',
                'ThumbnailURL',
                'ViewerCount'
            ]
            writer = csv.writer(file, delimiter='\t')
            writer.writerow(header)

    with tqdm(desc="Retrieving Streamers") as pbar:
        while True:
            response = requests.get(url, headers=headers, params=params)
            data = response.json()

            if 'data' in data:
                for streamer in data['data']:
                    save_streamer_to_csv(streamer, tsv_file_path)
                    save_count += 1
                    pbar.update(1)

            if 'pagination' in data and 'cursor' in data['pagination']:
                params['after'] = data['pagination']['cursor']
            else:
                break

    elapsed_time = time.time() - start_time
    average_speed = save_count / elapsed_time

    print(f'Average speed: {average_speed:.2f} streamers/second.')      
    print(f'Saved data for {save_count} streamers in total.')
    
streamers_data = get_twitch_streamers(access_token, first=100, tsv_file_path=tsv_file_path, save_interval=15000)