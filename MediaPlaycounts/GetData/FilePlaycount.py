import arrow
import os
import pymysql

directory = os.path.dirname(__file__)
sqlconfig = os.path.join(directory, "../.my.cnf")

def date(filename, date, db="s53189__mediaplaycounts_p", read_default_file=sqlconfig,
         host="tools-db", port=3306, success_log="success_log.txt", error_log="error_log.txt"):
    """
    Gets playcounts for an individual file on a specific date. Date must be a
    string in the format YYYYMMDD and the file must be without the "File:"
    prefix.
    """

    # Normalizing
    filename = filename.replace(" ", "_")

    # Constructing query
    q = "select viewcount from counts where file=%s and date=%s;"
    params = (filename, date)

    # Setting up DB connection
    conn = pymysql.connect(host=host,
                           port=port,
                           db=db,
                           read_default_file=read_default_file,
                           charset="utf8")
    cur = conn.cursor()
    cur.execute(q, params)

    data = []

    if cur.rowcount > 0:
        data.append({"filename": filename, "date": date, "count": cur.fetchone()[0]})

    conn.close()

    return data


def date_range(filename, start_date, end_date, db="s53189__mediaplaycounts_p",
               read_default_file=sqlconfig, host="tools-db", port=3306,
               success_log="success_log.txt", error_log="error_log.txt"):
    """
    Gets playcounts for an individual file for a range of dates, inclusive.
    Dates must be strings in the format YYYYMMDD and the file must be without
    the "File:" prefix.
    """

    # Normalizing
    filename = filename.replace(" ", "_")

    # Constructing query
    q = "select date, viewcount from counts where file=%s and date >= %s and date <= %s;"
    params = (filename, start_date, end_date)

    # Setting up DB connection
    conn = pymysql.connect(host=host,
                           port=port,
                           db=db,
                           read_default_file=read_default_file,
                           charset="utf8")
    cur = conn.cursor()
    cur.execute(q, params)

    data = []

    if cur.rowcount > 0:
        results = cur.fetchall()
        for result in results:
            data.append({"filename": filename, "date": result[0], "count": result[1]})

    conn.close()

    return data


def last_30(filename, db="s53189__mediaplaycounts_p",
            read_default_file=sqlconfig, host="tools-db", port=3306,
            success_log="success_log.txt", error_log="error_log.txt"):
    """
    Gets playcounts for an individual file for the last 30 days, starting with
    yesterday and going 30 days back from there. The file must be without the
    "File:" prefix.
    """

    yesterday = arrow.utcnow().replace(days=-1)
    and_29_days_before_that = yesterday.replace(days=-29).format("YYYYMMDD")
    yesterday = yesterday.format("YYYYMMDD")

    return date_range(filename, and_29_days_before_that, yesterday, db=db,
                      read_default_file=read_default_file, host=host, port=port,
                      success_log=success_log, error_log=error_log)

def last_90(filename, db="s53189__mediaplaycounts_p",
            read_default_file=sqlconfig, host="tools-db", port=3306,
            success_log="success_log.txt", error_log="error_log.txt"):
    """
    Gets playcounts for an individual file for the last 90 days, starting with
    yesterday and going 90 days back from there. The file must be without the
    "File:" prefix.
    """

    yesterday = arrow.utcnow().replace(days=-1)
    and_89_days_before_that = yesterday.replace(days=-89).format("YYYYMMDD")
    yesterday = yesterday.format("YYYYMMDD")

    return date_range(filename, and_89_days_before_that, yesterday, db=db,
                      read_default_file=read_default_file, host=host, port=port,
                      success_log=success_log, error_log=error_log)
