import arrow, bz2, os, re, requests, sys, urllib.parse
from helper import Helper

h = Helper()
DATE_REGEX = re.compile('^\d{8}$')


def parse(row):
    """
    Takes a line from a raw, decompressed log file and returns a tuple
    (filename, playcount)
    """

    ext_regex = re.compile('.*\.(mid|ogg|ogv|wav|webm|flac|oga)')

    columns = row.split('\t')
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
        components = base_name.split('/')

        # /wikipedia/commons/x/xx/FILENAME
        if len(components) == 6:
            if components[1] == 'wikipedia' and components[2] == 'commons':
                if len(components[3]) == 1 and len(components[4]) == 2:
                    filename = urllib.parse.unquote_plus(components[5])
                    if re.match(ext_regex, filename) is not None:
                        return (filename, int(playcount))


def download(date):
    """
    Downloads, decompresses, and parses a Mediacounts logfile from
    dumps.wikimedia.org and stores it in memory for parsing.
    Requires an Arrow date object.
    Yields a parsed line
    """
    root_url = 'https://dumps.wikimedia.org/other/mediacounts/daily/'
    date_string = date.format('YYYY-MM-DD')
    year_string = date.format('YYYY')
    filename = 'mediacounts.{0}.v00.tsv.bz2'.format(date_string)

    to_download = root_url + year_string + '/' + filename

    try:
        with open('/tmp/' + filename, 'wb') as f:
            f.write(requests.get(to_download).content)
        h.success_log('Downloaded: ' + to_download)
    except Exception as e:
        message = 'Failed to download ' + to_download + ' - ' + str(e)
        h.error_log(message)
        raise RuntimeError(message)

    with bz2.open('/tmp/' + filename) as f:
        for line in f:
            yield line.decode('utf-8')

    h.success_log('Processed ' + to_download)


def store(record, date):
    """
    Takes the output of the parse function (the output here being referred to as
    `record`) as well as the Arrow object representing the date of the log and
    stores it in Redis. Returns True on success. Raises an exception upon failure.
    """

    date_string = date.format('YYYYMMDD')

    for pair in record:
        h.redis.hincrby('mpc:' + pair[0], date_string, amount=pair[1])

    return True


def delete_file(date):
    """
    Takes an Arrow date object and deletes the corresponding file from the /tmp
    directory.
    """

    date_string = date.format('YYYY-MM-DD')
    filename = 'mediacounts.{0}.v00.tsv.bz2'.format(date_string)
    os.remove('/tmp/' + filename)


def run(dates=[arrow.utcnow().replace(days=-1)]):
    """
    Runs through the LogProcessor for the dates specified. Dates must be Arrow
    date objects.
    """

    for date in dates:
        record = []
        date_string = date.format('YYYY-MM-DD')
        print('Processing: ' + date_string)
        for line in download(date):
            line = parse(line)
            if line is not None:
                store([line], date)
        delete_file(date)


def delete_date(affected_date):
    """
    Deletes all values for a given date
    """

    date_string = affected_date.format('YYYYMMDD')

    try:
        for entry in h.redis.keys('mpc:*'):
            h.redis.hdel(entry, date_string)
        h.success_log('Deleted entries for: ' + date_string)
    except Exception as e:
        message = 'Failed to delete entries for ' + date_string + ': ' + str(e)
        h.error_log(message)
        raise e


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
            date_range = h.date_ranger(start_date='20150101')
            run(date_range)

        elif re.match(DATE_REGEX, args[0]) is None:
            raise ValueError('Invalid input: ' + args[0])

        day = [arrow.get(args[0], 'YYYYMMDD')]
        run(dates=day)

    elif len(args) == 2:
        # Two arguments: add data for the given date range
        # Unless the first word is "delete"

        if args[0] == 'delete':
            if re.match(DATE_REGEX, args[1]) is None:
                raise ValueError('Invalid input: ' + args[1])

            affected_date = arrow.get(args[1], 'YYYYMMDD')
            delete_date(affected_date)

        else:
            for arg in args:
                if re.match(DATE_REGEX, arg) is None:
                    raise ValueError('Invalid input: ' + arg)

            if args[0] > args[1]:
                raise ValueError(
                    'The first date must be before the second date')

            date_range = h.date_ranger(start_date=args[0], end_date=args[1])
            run(dates=date_range)

    else:
        raise RuntimeError('You put down too many parameters, dude')


if __name__ == '__main__':
    args = sys.argv[1:]
    process_args(args)
