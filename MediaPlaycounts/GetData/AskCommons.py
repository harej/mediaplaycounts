import pymysql

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
    should not have the "Category:" prefix.
    """

    if depth == 0:
        return None

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
        more = find_subcategories(val, depth=depth-1, db=db,
                           host=host, read_default_file=read_default_file, port=port,
                           success_log=success_log, error_log=error_log)
        if more != None:
            for more_result in more:
                categorylist.append(more_result)

    return list(set(categorylist)).sort()
