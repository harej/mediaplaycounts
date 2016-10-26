import arrow
import pymysql
from . import AskCommons

def _recursive_media_finder(category, depth=9, read_default_file="../.my.cnf",
                            host="commonswiki.labsdb", port=3306, db="commonswiki_p",
                            success_log="success_log.txt", error_log="error_log.txt"):
    """
    Helper function to generate a list of files to run database queries on.
    """

    manifest = []

    root_list = AskCommons.find_media_files(category, db=db, host=host,
                    read_default_file=read_default_file, port=port,
                    success_log=success_log, error_log=error_log)

    for entry in root_list:
        manifest.append(entry)

    subcat_list = AskCommons.find_subcategories(category, depth=depth, db=db,
                      host=host, read_default_file=read_default_file, port=port,
                      success_log=success_log, error_log=error_log)

    for subcat in subcat_list:
        subcat_file_list = AskCommons.find_media_files(subcat, db=db, host=host,
                               read_default_file=read_default_file, port=port,
                               success_log=success_log, error_log=error_log)

        for entry in subcat_file_list:
            manifest.append(entry)

    manifest = sorted(list(set(manifest)))

    return manifest

def date(category, date, depth=9, db="s53189__mediaplaycounts_p",
         read_default_file="../.my.cnf", host="tools-db", port=3306,
         commons_db="commonswiki_p", commons_host="commonswiki.labsdb", commons_port=3306,
         success_log="success_log.txt", error_log="error_log.txt"):
    """
    Gets playcounts for a category of files (with recursion) on a specific date.
    Date must be a string in the format YYYY-MM-DD and the category must be without
    the "Category:" prefix.
    """

    # Normalizing
    date = arrow.get(date)
    category = category.replace(" ", "_")



    return None



def date_range():
    return None

def last_30():
    return None

def last_90():
    return None

def all_time():
    return None
