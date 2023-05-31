from dotenv import load_dotenv
import os
import csv
import asyncio
import aiohttp
import time
from tqdm.asyncio import tqdm_asyncio as tqdm
import multiprocessing

load_dotenv()
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
access_token = os.getenv("ACCESS_TOKEN")

def save_user_to_csv(user_data, tsv_file_path):
    with open(tsv_file_path, 'a', newline='') as file:
        writer = csv.writer(file, delimiter='\t')
        writer.writerow([
            user_data['id'],
            user_data['login'],
            user_data['display_name'],
            user_data['type'],
            user_data['broadcaster_type'],
            user_data['description'],
            user_data['profile_image_url'],
            user_data['offline_image_url'],
            user_data['view_count'],
            user_data['created_at']
        ])

def read_columns_from_tsv(tsv_file_path, column_indices):
    if not os.path.exists(tsv_file_path):
        # Create an empty file
        with open(tsv_file_path, 'w'):
            pass

    column_values = []
    with open(tsv_file_path, 'r') as file:
        reader = csv.reader(file, delimiter='\t')
        try:
            header = next(reader)  # Read the header row
        except StopIteration:
            header = []  # Empty file, no header present
        for row in reader:
            values = [row[index] for index in column_indices]
            column_values.append(values)
    return column_values

def compare_unique_ids(tsv_file_path_1, tsv_file_path_2, column_indices_1, column_indices_2):
    unique_ids_1 = set(tuple(values) for values in read_columns_from_tsv(tsv_file_path_1, column_indices_1))
    unique_ids_2 = set(tuple(values) for values in read_columns_from_tsv(tsv_file_path_2, column_indices_2))
    unique_ids_correlated = unique_ids_1.intersection(unique_ids_2)  # Find the IDs that are present in both sets
    unique_ids_not_correlated = list(unique_ids_2 - unique_ids_correlated)  # Find the IDs in unique_ids_2 that are not in unique_ids_1
    count_user = len(unique_ids_1)
    count_before = len(unique_ids_2)
    count = len(unique_ids_not_correlated)
    return count_user, count_before, count, unique_ids_not_correlated

class TokenBucket:
    def __init__(self, rate_limit, bucket_size):
        self.rate_limit = rate_limit
        self.bucket_size = bucket_size
        self.tokens = bucket_size
        self.last_update = time.time()
        self.lock = asyncio.Lock()

    async def consume(self, tokens):
        async with self.lock:
            now = time.time()
            elapsed_time = now - self.last_update
            self.last_update = now
            self.tokens += elapsed_time * self.rate_limit
            self.tokens = min(self.tokens, self.bucket_size)
            if tokens <= self.tokens:
                self.tokens -= tokens
                return True
            return False

async def get_twitch_user(session, access_token, user_id, headers):
    url = f'https://api.twitch.tv/helix/users?id={user_id}'
    async with session.get(url, headers=headers) as response:
        data = await response.json()
        if 'data' in data and len(data['data']) > 0:
            return data['data'][0]
        else:
            return None

async def get_twitch_users(access_token, stream_ids, tsv_file_path, save_interval=1000):
    
    count_user, count_before, count, unique_ids_not_correlated = compare_unique_ids(tsv_file_path, tsv_file_path1, column_indices_1, column_indices_2)
    
    print("Total Unique ID on Stream Table: ", count_before)
    print("Total Unique ID on User Table: ", count_user)
    print("Total ID not Correlated into User_db: ", count)

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Client-Id': client_id
    }

    save_count = 0
    start_time = time.time()
    request_counter = 0

    if not os.path.isfile(tsv_file_path):
        with open(tsv_file_path, 'w', newline='') as file:
            header = [
                'id',
                'login',
                'display_name',
                'type',
                'broadcaster_type',
                'description',
                'profile_image_url',
                'offline_image_url',
                'view_count',
                'created_at'
            ]
            writer = csv.writer(file, delimiter='\t')
            writer.writerow(header)

    rate_limit = 10
    bucket_size = 30
    token_bucket = TokenBucket(rate_limit, bucket_size)

    concurrency_limit = multiprocessing.cpu_count() * 10 # Adjust the concurrency limit based on available CPU resources
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def fetch_user_info(session, user_id):
        url = f'https://api.twitch.tv/helix/users?id={user_id}'
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            if 'data' in data and len(data['data']) > 0:
                user_info = data['data'][0]
                save_user_to_csv(user_info, tsv_file_path)
                nonlocal save_count
                save_count += 1

    async def process_rows(session, row_ids):
        tasks = []
        for user_id in row_ids:
            while True:
                if await token_bucket.consume(1):
                    break
                await asyncio.sleep(0.1)

            await semaphore.acquire()
            task = asyncio.ensure_future(fetch_user_info(session, user_id))
            tasks.append(task)

            def release_semaphore(task):
                semaphore.release()

            task.add_done_callback(release_semaphore)

        # Gather and await all tasks concurrently
        await asyncio.gather(*tasks)

    async with aiohttp.ClientSession() as session:
        pbar = tqdm(total=len(stream_ids), desc="Retrieving Users")
        chunk_size = 10 * concurrency_limit
        for i in range(0, len(stream_ids), chunk_size):
            chunk_ids = stream_ids[i:i+chunk_size]
            await process_rows(session, chunk_ids)
            pbar.update(len(chunk_ids))

            if save_count % save_interval == 0:
                await asyncio.sleep(0)  # Yield control to allow other tasks to execute

            request_counter += len(chunk_ids)
            if request_counter % 30 == 0:
                elapsed_time = time.time() - start_time
                if elapsed_time < 1:
                    sleep_duration = max(0.5 - elapsed_time, 0)
                    await asyncio.sleep(sleep_duration)
                    start_time = time.time()
                    request_counter = 0

    elapsed_time = time.time() - start_time
    average_speed = save_count / elapsed_time
    print(f'Average speed: {average_speed:.2f} users/second.')
    print(f'Saved data for {save_count} users in total.')
    pbar.close()  # Close the progress bar


# Read stream IDs from the TSV file
stream_ids = [item[0] for item in compare_unique_ids(tsv_file_path, tsv_file_path1, column_indices_1, column_indices_2)[-1]]

save_interval = 1000  # Optional: Specify the save interval
asyncio.run(get_twitch_users(access_token, stream_ids, tsv_file_path, save_interval))
