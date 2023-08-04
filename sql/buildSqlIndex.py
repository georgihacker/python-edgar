import sqlite3


EDGAR_BASE = "https://www.sec.gov/Archives/"

def parse(line):
    # each line: "cik|firm_name|file_type|date|url_txt|url_html"
    # an example:
    # "99780|TRINITY INDUSTRIES INC|8-K|2020-01-15|edgar/data/99780/0000099780-\
    # 20-000008.txt|edgar/data/99780/0000099780-20-000008-index.html"
    line = tuple(line.split('|')[:5])
    l = list(line)
    l[-1] = EDGAR_BASE + l[-1]
    return tuple(l)

if __name__ == '__main__':
    conn = sqlite3.connect(r"edgar-idx.sqlite3")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS edgar_idx 
        (cik TEXT, firm_name TEXT, file_type TEXT, date DATE, url TEXT,
        PRIMARY KEY(cik, file_type, date));''')

    filename = '../edgar-idx/master.tsv'
    with open(filename, 'r') as f:
        lines = f.readlines()

    data = [parse(line) for line in lines]
    c.executemany('INSERT OR IGNORE INTO edgar_idx \
        (cik, firm_name, file_type, date, url) VALUES (?,?,?,?,?)', data)

    conn.commit()
    conn.close()