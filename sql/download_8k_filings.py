# Download all 8-K filings.

import os
import sqlite3
import requests
import concurrent.futures
import gzip
import tqdm

def download(job):
    cik, _, file_type, date, url = job
    try:
        res = requests.get(url)
        filename = f'./data/{cik}/{file_type}/{date}.txt.gz'
        if res.status_code == 200:
            with gzip.open(filename, 'wb') as f:
                f.write(res.content)
    except Exception:
        pass

if __name__ == "__main__":
    # select what to download
    conn = sqlite3.connect(r"edgar-idx.sqlite3")
    c = conn.cursor()
    c.execute('SELECT * FROM edgar_idx WHERE file_type="8-K";')
    jobs = c.fetchall()
    conn.close()
    # start downloading
    progress = tqdm.tqdm(total=len(jobs))
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as exe:
        for job in jobs:
            cik, _, file_type, date, url = job
            filename = f'./data/{cik}/{file_type}/{date}.txt.gz'
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            if os.path.exists(filename):
                progress.update()
            else:
                f = exe.submit(download, job)
                f.add_done_callback(lambda _: progress.update())
                futures.append(f)
    for f in concurrent.futures.as_completed(futures):
        pass