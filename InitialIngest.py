import arrow
from MediaPlaycounts import LogProcessor

def generate_dates():
    """
    Returns a list of Arrow date objects for each day since 1 January 2015 when
    these logs begin.
    """
    return arrow.Arrow.range('day', arrow.get('2015-01-01'), arrow.now())

def run(read_default_file, db="mediaplaycounts", dates=generate_dates(),
        host="localhost", port=3306):

    for date in dates:
    	date_string = date.format('YYYY-MM-DD')
    	print("Downloading: " + date_string)
    	raw_log = LogProcessor.download(date)
    	print("Parsing: " + date_string)
    	record = LogProcessor.parse(raw_log)
    	print("Recording: " + date_string)
    	LogProcessor.store(record, date, db, read_default_file, host, port)
    
if __name__ == '__main__':
    run("../.my.cnf", db="s53189__mediaplaycounts_p", host="tools-db")