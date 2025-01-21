import os
import pandas as pd
import requests
import json
import time
import subprocess
import sys
import logging
from tqdm import tqdm


class VideoDownloader:
    def __init__(self, ip_url, csv_file_path, output_dir, batch_name):
        self.ip_url = ip_url
        self.csv_file_path = csv_file_path
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.error_log_path = os.path.join(output_dir, 'error_log.txt')
        self.progress_log_path = os.path.join(output_dir, 'progress_log.txt')
        log_file_path = os.path.join(output_dir, f'{batch_name}_log.txt')
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s - %(message)s', 
                            filename=log_file_path,  # Save the log to a file
                            filemode='a')
        logging.info("VideoDownloader initialized.")

    def log_error(self, avid):
        with open(self.error_log_path, 'a') as f:
            f.write(f"{avid}\n")
        logging.error(f"Recorded error for avid: {avid}")

    def log_progress(self, completed_avid):
        with open(self.progress_log_path, 'a') as f:
            f.write(f"{completed_avid}\n")

    def load_progress(self):
        if os.path.exists(self.progress_log_path):
            with open(self.progress_log_path, 'r') as f:
                return set(int(line.strip()) for line in f if line.strip())
        return set()

    def get_ip(self, ip_url):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                logging.info(f"Attempting to fetch IP (Attempt {attempt + 1})...")
                ip = requests.get(ip_url).text
                ip = json.loads(ip)
                if ip['ret'] == 200:
                    logging.info(f"Successfully fetched IP: {ip['data'][0]}")
                    break
                logging.warning("Failed to get IP, retrying...")
                time.sleep(5)
            except Exception as e:
                logging.error(f"Error getting IP: {e}")
                time.sleep(5)
        else:
            logging.error("Max retries reached, exiting.")
            sys.exit(1)

        proxy = "http://%(ip)s:%(port)s" % {
            "ip": ip['data'][0]['ip'],
            "port": ip['data'][0]['port']
        }
        self.proxy = proxy
        self.proxy_fetch_time = time.time()  # Record the time the IP was fetched
        logging.info(f"Using proxy: {proxy}")
        return proxy

    def check_ip_validity(self):
        if self.proxy is None or (time.time() - self.proxy_fetch_time) > 300:  
            logging.info("Proxy IP is expired or not set, fetching a new one.")
            self.proxy = self.get_ip(self.ip_url)
    
    def run(self):
        try:
            logging.info(f"Reading CSV file: {self.csv_file_path}")
            avid_df = pd.read_csv(self.csv_file_path)
            avid_list = avid_df['avid'].unique().tolist()
            completed_avids = self.load_progress()

            remaining_avids = [avid for avid in avid_list if avid not in completed_avids]
            logging.info(f"Total {len(avid_list)} videos, {len(remaining_avids)} remaining.")
            total_videos = len(avid_list)
            completed_count = len(completed_avids)
           #proxy = self.get_ip(self.ip_url)  # Fetch the initial proxy
            self.get_ip(self.ip_url)

            with tqdm(total=total_videos, initial=completed_count, desc="Downloading Videos", unit="video") as pbar:
                for index, avid in enumerate(remaining_avids, start=1):
                    self.check_ip_validity() 
                    
                    logging.info(f"Processing avid {avid} ({index}/{len(remaining_avids)})...")
                    try:
                        merge_path, audio_only_path, video_only_path = self.create_directories(avid)
                        self.generate_and_run_commands(avid, self.proxy, merge_path, audio_only_path, video_only_path)
                        
                        for root, _, files in os.walk(merge_path):
                            for file in files:
                                if file.endswith('.mp4'):
                                    video_path = os.path.join(root, file)
                                    self.extract_cover_image(video_path)
                        self.log_progress(avid)
                        
                        pbar.update(1)

                        if index % 1000 == 0:
                            elapsed_time = time.time() - self.start_time
                            print(f"Downloaded {index} files in {elapsed_time:.2f} seconds. Press any key to continue...")
                            input()
                        
                    except Exception as e:
                        logging.error(f"An error occurred for avid {avid}: {e}")
                        self.log_error(avid)  # 
        except FileNotFoundError as e:
            logging.error(f"CSV file not found: {self.csv_file_path}. Error: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            sys.exit(1)


    def generate_and_run_commands(self, avid, proxy, merge_path, audio_only_path, video_only_path):
            base_url = f"https://www.bilibili.com/video/av{avid}/"
            
            commands = [
                f"yutto {base_url} --with-metadata -d {merge_path} --proxy {proxy} --no-progress -w"
            ]
            
            for command in commands:
                logging.info(f"Executing command: {command}")
                try:
                    result = subprocess.run(command, shell=True, timeout=180)
                    if result.returncode != 0:
                        logging.warning(f"Command failed with return code {result.returncode}. Command: {command}")
                        self.log_error(avid)
                except subprocess.TimeoutExpired:
                    logging.warning(f"Download command timed out for avid {avid}. Command: {command}")
                    self.log_error(avid)
                except Exception as e:
                    logging.error(f"Unexpected error while executing command for avid {avid}: {e}")
                    self.log_error(avid)

            
    def extract_cover_image(self, video_path):
        output_path = os.path.splitext(video_path)[0] + '.jpg'
        command = [
            'ffmpeg', '-loglevel', 'quiet', '-i', video_path,
            '-map', '0:v', '-map', '-0:V', '-c', 'copy',
            output_path
        ]
        start_time = time.time()  
        try:
            result = subprocess.run(command, check=True, timeout=60)  # Run the command and check for errors
            end_time = time.time()  # Record the time the extraction was completed
            elapsed_time = end_time - start_time  # Calculate the time taken for extraction
            logging.info(f'Successfully extracted cover from {video_path} to {output_path}')
            logging.info(f'Extraction successful at: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))}')
            logging.info(f'Time taken for extraction: {elapsed_time:.2f} seconds')
        
        except subprocess.CalledProcessError as e:
            logging.error(f'Error extracting cover from {video_path}: {e}')
            self.log_error(os.path.basename(video_path))

    def create_directories(self, avid):
        merge_path = os.path.join(self.output_dir, str(avid), 'merge')
        audio_only_path = os.path.join(self.output_dir, str(avid), 'audio_only')
        video_only_path = os.path.join(self.output_dir, str(avid), 'video_only')
        os.makedirs(merge_path, exist_ok=True)
        os.makedirs(audio_only_path, exist_ok=True)
        os.makedirs(video_only_path, exist_ok=True)
        logging.info(f"Created directories for avid {avid}: {merge_path}, {audio_only_path}, {video_only_path}")
        return merge_path, audio_only_path, video_only_path

if __name__ == '__main__':
    batch_name = sys.argv[1]
    
    ip_url = "your ip"
    csv_file_path = f"{batch_name}"
    output_dir = f"small_output/{batch_name}"
    
    downloader = VideoDownloader(ip_url, csv_file_path, output_dir, batch_name)
    downloader.start_time = time.time()
    downloader.run()

