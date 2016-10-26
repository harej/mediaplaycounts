import pymysql
import re

def _query(query, params, db="commonswiki_p", host="commonswiki.labsdb",
           read_default_file="../.my.cnf", port=3306,
           success_log="success_log.txt", error_log="error_log.txt"):
    """
    Helper function to perform database queries
    """

    conn = pymysql.connect(host=host,
                           port=port,
                           db=db,
                           read_default_file=read_default_file,
                           charset="utf8")
    cur = conn.cursor()
    cur.execute(query, params)

    data = []

    if cur.rowcount > 0:
        results = cur.fetchall()
        for result in results:
            data.append(result)

    conn.close()

    return data

def find_subcategories(category, depth=9, db="commonswiki_p",
                       host="commonswiki.labsdb", read_default_file="../.my.cnf",
                       port=3306, success_log="success_log.txt", error_log="error_log.txt"):
    """
    Finds subcategories of a given category up to the provided depth. Category
    should not have the "Category:" prefix. Returns a flat list of categories.
    """

    if depth == 0:
        return [category]

    categorylist = []

    query = ("select page_title from categorylinks join page on cl_from = page_id"
             " where cl_to = %s and cl_type = 'subcat'")
    params = (category.replace(" ", "_"))

    results = _query(query, params, db="commonswiki_p", host="commonswiki.labsdb",
                     read_default_file=read_default_file, port=3306,
                     success_log="success_log.txt", error_log="error_log.txt")

    for result in results:
        val = result[0].decode('utf-8')
        categorylist.append(val)
        more = find_subcategories(val, depth=depth-1, db=db, host=host,
                                  read_default_file=read_default_file, port=port,
                                  success_log=success_log, error_log=error_log)
        if more != None:
            for more_result in more:
                categorylist.append(more_result)

    categorylist = sorted(list(set(categorylist)))
    return categorylist

def find_media_files(category, db="commonswiki_p", host="commonswiki.labsdb",
                     read_default_file="../.my.cnf", port=3306,
                     success_log="success_log.txt", error_log="error_log.txt"):
    """
    Generates a list of media files for a single category. Use the find_subcategories
    method to generate a list of subcategories to feed individually into this
    method. Note that the returned files do not include still images; only videos
    and the like are returned.
    """

    filelist = []

    q = ("select page_title from page join categorylinks on cl_from = page_id "
         "where page_namespace=6 and cl_to = %s;")

    params = (category.replace(" ", "_"))

    results = _query(q, params, db="commonswiki_p", host="commonswiki.labsdb",
                     read_default_file=read_default_file, port=3306,
                     success_log="success_log.txt", error_log="error_log.txt")

    ext_regex = re.compile('.*\.(mid|ogg|ogv|wav|webm|flac|oga)')
    for result in results:
        filename = result[0].decode("utf-8")
        if re.match(ext_regex, filename) != None:
            filelist.append(filename)

    filelist.sort()

    return filelist
