import arrow
from MediaPlaycounts import LogProcessor

def run(read_default_file, db="mediaplaycounts",
        dates=[arrow.utcnow().replace(days=-1)], host="localhost", port=3306):
    """
    Runs through the LogProcessor for the dates specified. Dates must be Arrow
    date objects.
    """

    for date in dates:
        record = []
        date_string = date.format('YYYY-MM-DD')
        for line in LogProcessor.download(date):
            if line is not None:
                record.append(line)
        LogProcessor.store(record, date, db, read_default_file, host, port)

if __name__ == '__main__':
    run(".my.cnf", db="s53189__mediaplaycounts_p", host="tools-db")
