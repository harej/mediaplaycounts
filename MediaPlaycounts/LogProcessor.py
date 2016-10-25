import arrow
import bz2
import csv
import pymysql
import re
import requests
import urllib.parse
from . import WorkLogger

def download(date, success_log="success_log.txt", error_log="error_log.txt"):
    """
    Downloads and decompresses a Mediacounts logfile from dumps.wikimedia.org
    and stores it in memory for parsing.
    Requires an Arrow date object.
    Returns a relative path to the file.
    """
    root_url = "https://dumps.wikimedia.org/other/mediacounts/daily/"
    filename = "mediacounts.{0}.v00.tsv.bz2"
    date_string = date.format('YYYY-MM-DD')
    year_string = date.format('YYYY')

    to_download = root_url + year_string + "/" + filename.format(date_string)

    try:
        downloaded_file = requests.get(to_download).content
    except Exception as e:
        message = "Failed to download " + to_download + " - " + e
        WorkLogger.error_log(message, error_log)

    try:
        decompressed = bz2.decompress(downloaded_file)
    except Exception as e:
        message = "Failed to decompress " + to_download + " - " + e
        WorkLogger.error_log(message, error_log)

    WorkLogger.success_log("Downloaded " + to_download, success_log)

    return decompressed.decode('utf-8')


def parse(raw_feed, success_log="success_log.txt", error_log="error_log.txt"):
    """
    Takes a raw, decompressed log file in string form, and returns a dictionary
    (file: count)
    """

    result = []

    ext_regex = re.compile('.*\.(mid|ogg|ogv|wav|webm|flac|oga)')

    spreadsheet = raw_feed.split("\n")
    for row in spreadsheet:
        columns = row.split("\t")
        if len(columns) < 2:  # Not a real row
            continue
        base_name = columns[0]

        if columns[3] == '-':
            columns[3] = 0
        if columns[4] == '-':
            columns[4] = 0
        if columns[16] == '-':
            columns[16] = 0

        playcount = int(columns[3]) + int(columns[4]) + int(columns[16])


        # First we must determine if this is a media file
        components = base_name.split("/")

        # /wikipedia/commons/x/xx/FILENAME
        if len(components) == 6:
            if components[1] == "wikipedia" and components[2] == "commons":
                if len(components[3]) == 1 and len(components[4]) == 2:
                    filename = urllib.parse.unquote_plus(components[5])
                    if re.match(ext_regex, filename) != None:
                        result.append((filename, int(playcount)))

    WorkLogger.success_log("Parsed log and generated " + str(len(result)) +
                           " records", success_log)

    return result

def store(record, date, db, read_default_file, host="localhost", port=3306,
          success_log="success_log.txt", error_log="error_log.txt"):
    """
    Takes the output of the parse function (the output here being referred to as
    `record`) as well as the Arrow object representing the date of the log and
    stores it in a MySQL database. Returns True on success. Raises an exception
    upon failure.
    """

    # create table `counts` 
    # ( `id` int(11) not null auto_increment primary key, 
    # `date` varchar(255) collate utf8_bin not null,
    # `file` varchar(255) collate utf8_bin not null,
    # `viewcount` int(11) not null )
    # engine=InnoDB default charset=utf8 collate=utf8_bin;

    date_string = date.format('YYYYMMDD')

    packages = [record[x:x+10000] for x in range(0, len(record), 10000)]

    for package in packages:
        conn = pymysql.connect(host=host,
                               port=port,
                               db=db,
                               read_default_file=read_default_file,
                               charset='utf8')
        sqlquery = "insert into `counts` (`date`, `file`, `viewcount`) values "
        megatuple = []  # will be converted into a tuple
        for pair in package:
            file_name = pair[0]
            count = pair[1]
            sqlquery += "(%s, %s, %s), "
            megatuple.append(date_string)
            megatuple.append(file_name)
            megatuple.append(count)

        sqlquery = sqlquery[:-2]
        sqlquery += ";"
        megatuple = tuple(megatuple)

        cur = conn.cursor()
        cur.execute(sqlquery, megatuple)
        conn.commit()
        conn.close()

    WorkLogger.success_log("Added " + str(len(record)) + " records to database",
                           success_log)

    return True
