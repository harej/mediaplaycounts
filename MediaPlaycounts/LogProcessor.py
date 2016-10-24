import arrow
import bz2
import csv
import pymysql
import re
import requests
import urllib.parse

def download(date):
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

    downloaded_file = requests.get(to_download).content
    decompressed = bz2.decompress(downloaded_file)
    return decompressed.decode('utf-8')


def parse(raw_log):
    """
    Takes a raw, decompressed log file in string form, and returns a dictionary
    (file: count)
    """

    result = []

    ext_regex = re.compile('.*\.(mid|ogg|ogv|wav|webm|flac|oga)')

    spreadsheet = raw_log.split("\n")
    for row in spreadsheet:
        columns = row.split("\t")
        if len(columns) < 2:  # Not a real row
            continue
        base_name = columns[0]
        playcount = columns[2]

        # First we must determine if this is a media file
        components = base_name.split("/")

        # /wikipedia/commons/x/xx/FILENAME
        if len(components) == 6:
            if components[1] == "wikipedia" and components[2] == "commons":
                if len(components[3]) == 1 and len(components[4]) == 2:
                    filename = urllib.parse.unquote_plus(components[5])
                    if re.match(ext_regex, filename) != None:
                        result.append((filename, int(playcount)))

    return result

def store(record, date, db, read_default_file, host="localhost", port=3306):
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
    # `count` int(11) not null )
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

    return True
