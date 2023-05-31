from dotenv import load_dotenv
import requests
import os
from datetime import datetime
import csv
from tqdm import tqdm
import time
import subprocess

load_dotenv()
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
access_token = os.getenv("ACCESS_TOKEN")

tsv_file_path = "/home/mocha/cap/finalproj/CAP_Final_Proj/stream_0.tsv"
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
unified_origin_file_path = f"/home/mocha/cap/finalproj/CAP_Final_Proj/stream_{timestamp}.tsv"
hdfs_path = f'/user/mocha/final/stream/stream_{timestamp}.tsv'

def save_streamer_to_csv(streamer, tsv_file_path, unified_file_path):
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

    # Append the data to the unified origin data file
    with open(unified_file_path, 'a', newline='') as unified_file:
        writer = csv.writer(unified_file, delimiter='\t')
        writer.writerow(row)


def get_twitch_streamers(access_token, first, tsv_file_path, unified_file_path):
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

    with tqdm(desc="Retrieving Streamers") as pbar:
        while True:
            response = requests.get(url, headers=headers, params=params)
            data = response.json()

            if 'data' in data:
                for streamer in data['data']:
                    save_streamer_to_csv(streamer, tsv_file_path, unified_file_path)
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

def write_to_hdfs(file_path, hdfs_path):
    bash_command = f'hadoop fs -put {file_path} {hdfs_path}'
    subprocess.run(bash_command, shell=True)

# Create the unified origin data file
with open(unified_origin_file_path, 'w', newline='') as unified_origin_file:
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
    writer = csv.writer(unified_origin_file, delimiter='\t')
    writer.writerow(header)

# Call the function to retrieve Twitch streamers and save the data
streamers_data = get_twitch_streamers(access_token, first=100, tsv_file_path=tsv_file_path, unified_file_path=unified_origin_file_path)

write_to_hdfs(unified_origin_file_path, hdfs_path)