import arrow
import bz2
import re
import requests
import redis
import sys
import urllib.parse
import WorkLogger
import config

REDIS = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)
success_log = config.SUCCESS_LOG
error_log = config.ERROR_LOG
DATE_REGEX = re.compile('^\d{4}-\d{2}-\d{2}$')

def parse(row):
    """
    Takes a line from a raw, decompressed log file and returns a tuple
    (filename, playcount)
    """

    ext_regex = re.compile('.*\.(mid|ogg|ogv|wav|webm|flac|oga)')

    columns = row.split("\t")
    if len(columns) < 2:  # Not a real row
        return
    base_name = columns[0]

    if columns[3] == '-':
        columns[3] = 0
    if columns[4] == '-':
        columns[4] = 0
    if columns[16] == '-':
        columns[16] = 0

    playcount = int(columns[3]) + int(columns[4]) + int(columns[16])

    if playcount > 0:
        # First we must determine if this is a media file
        components = base_name.split("/")

        # /wikipedia/commons/x/xx/FILENAME
        if len(components) == 6:
            if components[1] == "wikipedia" and components[2] == "commons":
                if len(components[3]) == 1 and len(components[4]) == 2:
                    filename = urllib.parse.unquote_plus(components[5])
                    if re.match(ext_regex, filename) is not None:
                        return (filename, int(playcount))

def download(date):
    """
    Downloads, decompresses, and parses a Mediacounts logfile from dumps.wikimedia.org
    and stores it in memory for parsing.
    Requires an Arrow date object.
    Yields a parsed line
    """
    root_url = "https://dumps.wikimedia.org/other/mediacounts/daily/"
    filename = "mediacounts.{0}.v00.tsv.bz2"
    date_string = date.format('YYYY-MM-DD')
    year_string = date.format('YYYY')

    to_download = root_url + year_string + "/" + filename.format(date_string)

    try:
        downloaded_file = requests.get(to_download).content
    except Exception as e:
        message = "Failed to download " + to_download + " - " + str(e)
        WorkLogger.error_log(message, error_log)
        raise RuntimeError(message)

    try:
        decompressed = bz2.decompress(downloaded_file)
    except Exception as e:
        message = "Failed to decompress " + to_download + " - " + str(e)
        WorkLogger.error_log(message, error_log)
        raise RuntimeError(message)

    for line in re.finditer(r'.+', decompressed.decode('utf-8')):
        yield parse(line.group(0))

    WorkLogger.success_log("Processed " + to_download, success_log)

def store(record, date):
    """
    Takes the output of the parse function (the output here being referred to as
    `record`) as well as the Arrow object representing the date of the log and
    stores it in Redis. Returns True on success. Raises an exception upon failure.
    """

    date_string = date.format('YYYYMMDD')

    for pair in record:
        REDIS.hincrby('mpc:' + pair[0], date_string, amount=pair[1])

    return True

def generate_dates(begin=arrow.get('2015-01-01'), end=arrow.utcnow()):
    """
    Returns a list of Arrow date objects for each day since 1 January 2015 when
    these logs begin.
    """
    return arrow.Arrow.range('day', begin, end)
    
def run(dates=[arrow.utcnow().replace(days=-1)]):
    """
    Runs through the LogProcessor for the dates specified. Dates must be Arrow
    date objects.
    """

    for date in dates:
        record = []
        date_string = date.format('YYYY-MM-DD')
        print("Processing: " + date_string)
        for line in download(date):
            if line is not None:
                store([line], date)

def process_args(args):
    """
    Processes command line arguments.
    """

    if len(args) == 0:
        # No arguments: add data for the past day.
        run()

    elif len(args) == 1:
        # One argument: add data for the specified day.
        # Unless that argument is "initial"

        if args[0] == 'initial':
            run(generate_dates())

        elif re.match(DATE_REGEX, args[0]) is None:
            raise ValueError('Invalid input: ' + args[0])

        day = arrow.get(args[0])
        run(dates=[day])

    elif len(args) == 2:
        # Two arguments: add data for the given date range
        if re.match(DATE_REGEX, args[0]) is None:
            raise ValueError('Invalid input: ' + args[0])
        if re.match(DATE_REGEX, args[1]) is None:
            raise ValueError('Invalid input: ' + args[1])

        if args[0] > args[1]:
            raise ValueError('The first date must be before the second date')

        begin = arrow.get(args[0])
        end = arrow.get(args[1])

        date_range = generate_dates(begin, end)
        run(date_range)

    else:
        raise RuntimeError('You put down too many parameters, dude')

if __name__ == '__main__':
    args = sys.argv[1:]
    process_args(args)
