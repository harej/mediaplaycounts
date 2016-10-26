import arrow
import pymysql

def date(category, date, depth=9, db=s53189__mediaplaycounts_p,
         read_default_file=read_default_file, host="tools-db", port=3306,
         commons_db="commonswiki_p", commons_host="commonswiki.labsdb", commons_port=3306,
         success_log="success_log.txt", error_log="error_log.txt"):
    """
    Gets playcounts for a category of files (with recursion) on a specific date.
    Date must be a string in the format YYYY-MM-DD and the category must be without
    the "Category:" prefix.
    """

    # Normalizing
    date = arrow.get(date)
    filename = filename.replace(" ", "_")

    return None



def date_range():
    return None

def last_30():
    return None

def last_90():
    return None

def all_time():
    return None
