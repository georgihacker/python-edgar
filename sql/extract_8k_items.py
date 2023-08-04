# Extract all items reported in 8-K filings since 2004.
import os
import gzip
import tqdm
import sqlite3
import concurrent.futures


BASE_DIR = './data'
FILE_TYPE = '8-K'
DB = "result.sqlite3"


def walk_dirpath(cik, file_type):
    """ Yield paths of all files for a given cik and file type """
    for root, _, files in os.walk(os.path.join(BASE_DIR, cik, file_type)):
        for filename in files:
            yield os.path.join(root, filename)


def regsearch(cik):
    matches = []
    for filepath in walk_dirpath(cik, FILE_TYPE):
        date = os.path.split(filepath)[1].strip('.txt.gz')
        if int(date.split('-')[0]) < 2004:
            continue
        with gzip.open(filepath, 'rb') as f:
            data = f.readlines()
        ls = [l for l in data if l.startswith(b'ITEM INFORMATION')]
        for l in ls:
            item = l.decode().replace('\t','').replace('ITEM INFORMATION:', '')
            if len(item.strip()):
                matches.append((cik, FILE_TYPE, date, item.strip()))
    return matches


if __name__ == "__main__":
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS files_all_items
        (cik TEXT, file_type TEXT, date DATE, item TEXT,
        PRIMARY KEY(cik, file_type, date, item));''')
    conn.commit()

    _, ciks, _ = next(os.walk(BASE_DIR))
    progress = tqdm.tqdm(total=len(ciks))
    with concurrent.futures.ProcessPoolExecutor(max_workers=16) as exe:
        futures = [exe.submit(regsearch, cik) for cik in ciks]
        for f in concurrent.futures.as_completed(futures):
            res = f.result()
            c.executemany(
                "INSERT OR IGNORE INTO files_all_items \
                    (cik, file_type, date, item) VALUES (?,?,?,?)", res)
            conn.commit()
            progress.update()

    conn.close()