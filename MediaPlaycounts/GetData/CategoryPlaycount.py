import os
from . import AskCommons, FilePlaycount

directory = os.path.dirname(__file__)
sqlconfig = os.path.join(directory, "../../.my.cnf")

def _recursive_media_finder(category, depth=9, read_default_file=sqlconfig,
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
         read_default_file=sqlconfig, host="tools-db", port=3306,
         commons_db="commonswiki_p", commons_host="commonswiki.labsdb", commons_port=3306,
         success_log="success_log.txt", error_log="error_log.txt"):
    """
    Gets playcounts for a category of files (with recursion) on a specific date.
    Date must be a string in the format YYYYMMDD and the category must be without
    the "Category:" prefix.
    """

    output = []

    # Normalizing
    category = category.replace(" ", "_")

    file_list = _recursive_media_finder(category, depth=depth,
                    read_default_file=read_default_file,
                    host=commons_host, port=3306, db=commons_db,
                    success_log=success_log, error_log=error_log)

    total = 0
    for filename in file_list:
        subquery = FilePlaycount.date(filename, date, db=db,
                       read_default_file=read_default_file, host=host, port=port,
                       success_log=success_log, error_log=error_log)
        output.append({"filename": filename, "details": subquery[0]})
    for triplet in output:
        total += triplet["count"]

    return {"total": total, "details": output}

def date_range(category, start_date, end_date, depth=9, db="s53189__mediaplaycounts_p",
                  read_default_file=sqlconfig, host="tools-db", port=3306,
                  commons_db="commonswiki_p", commons_host="commonswiki.labsdb",
                  commons_port=3306, success_log="success_log.txt", error_log="error_log.txt"):

    """
    Gets playcounts for a category of files (with recursion) for a range of dates,
    inclusive. Date must be a string in the format YYYYMMDD and the category must
    be without the "Category:" prefix.
    """

    output = []
    total = 0

    # Normalizing
    category = category.replace(" ", "_")

    file_list = _recursive_media_finder(category, depth=depth,
                    read_default_file=read_default_file,
                    host=commons_host, port=3306, db=commons_db,
                    success_log=success_log, error_log=error_log)

    for filename in file_list:
        subquery = FilePlaycount.date_range(filename, start_date, end_date, db=db,
                       read_default_file=read_default_file, host=host, port=port,
                       success_log=success_log, error_log=error_log)
        subtotal = subquery["total"]

        output.append({"total": subtotal, "details": subquery})

    for blob in output:
        total += blob["total"]

    return {"total": total, "details": output}

def last_30(category, depth=9, db="s53189__mediaplaycounts_p",
               read_default_file=sqlconfig, host="tools-db", port=3306,
               commons_db="commonswiki_p", commons_host="commonswiki.labsdb",
               commons_port=3306, success_log="success_log.txt", error_log="error_log.txt"):

    """
    Gets playcounts for a category of files (with recursion) for the last 30 days,
    starting with yesterday and going back 30 days from there. The category must
    be without the "Category:" prefix.
    """

    output = []

    # Normalizing
    category = category.replace(" ", "_")

    file_list = _recursive_media_finder(category, depth=depth,
                    read_default_file=read_default_file,
                    host=commons_host, port=3306, db=commons_db,
                    success_log=success_log, error_log=error_log)

    for filename in file_list:
        subquery = FilePlaycount.last_30(filename, db=db,
                       read_default_file=read_default_file, host=host, port=port,
                       success_log=success_log, error_log=error_log)
        for result in subquery:
            output.append(result)

    return output

def last_90(category, depth=9, db="s53189__mediaplaycounts_p",
               read_default_file=sqlconfig, host="tools-db", port=3306,
               commons_db="commonswiki_p", commons_host="commonswiki.labsdb",
               commons_port=3306, success_log="success_log.txt", error_log="error_log.txt"):

    """
    Gets playcounts for a category of files (with recursion) for the last 90 days,
    starting with yesterday and going back 30 days from there. The category must
    be without the "Category:" prefix.
    """

    output = []

    # Normalizing
    category = category.replace(" ", "_")

    file_list = _recursive_media_finder(category, depth=depth,
                    read_default_file=read_default_file,
                    host=commons_host, port=3306, db=commons_db,
                    success_log=success_log, error_log=error_log)

    for filename in file_list:
        subquery = FilePlaycount.last_90(filename, db=db,
                       read_default_file=read_default_file, host=host, port=port,
                       success_log=success_log, error_log=error_log)
        for result in subquery:
            output.append(result)

    return output
