import arrow
import pymysql
import re
import config

REDIS = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)
success_log = config.SUCCESS_LOG
error_log = config.ERROR_LOG

def _query_commons(query, params):
    """
    Helper function to perform database queries
    """

    conn = pymysql.connect(
        host=config.COMMONS_HOST,
        port=config.COMMONS_PORT,
        db=config.COMMONS_DB,
        user=config.SQL_USER,
        password=config.SQL_PASS,
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

def _date_ranger(start_date=None, end_date=None, last=None):
    """
    Helper function to take whatever date input the user gives and turn it into
    a range of Arrow objects
    """

    if end_date is None:
        arrow.utcnow().replace(days=-1)
    else:
        end_date = arrow.get(end_date, 'YYYYMMDD')

    if start_date is not None:
        start_date = arrow.get(start_date, 'YYYYMMDD')
    elif last is not None:
        amount = (last * -1) + 1
        start_date = end_date.replace(days=amount).format('YYYYMMDD')
    else:
        start_date = end_date

    return arrow.Arrow.range('day', start_date, end_date)

def _find_subcategories(category, depth=9):
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

    results = _query_commons(query, params)

    for result in results:
        val = result[0].decode('utf-8')
        categorylist.append(val)
        more = _find_subcategories(val, depth=depth-1)
        if more != None:
            for more_result in more:
                categorylist.append(more_result)

    categorylist = sorted(list(set(categorylist)))
    return categorylist

def _find_media_files(category):
    """
    Generates a list of media files for a single category. Use the _find_subcategories
    method to generate a list of subcategories to feed individually into this
    method. Note that the returned files do not include still images; only videos
    and the like are returned.
    """

    filelist = []

    q = ("select page_title from page join categorylinks on cl_from = page_id "
         "where page_namespace=6 and cl_to = %s;")

    params = (category.replace(" ", "_"))
    results = _query_commons(q, params)
    ext_regex = re.compile('.*\.(mid|ogg|ogv|wav|webm|flac|oga)')
    for result in results:
        filename = result[0].decode("utf-8")
        if re.match(ext_regex, filename) != None:
            filelist.append(filename)

    filelist.sort()

    return filelist

def _recursive_file_finder(category, depth=9):
    """
    Recursively goes through categories (up to the depth provided) and produces
    a list of files.
    """

    manifest = []

    root_list = _find_media_files(category)
    for entry in root_list:
        manifest.append(entry)

    subcat_list = _find_subcategories(category, depth=depth)
    for subcat in subcat_list:
        subcat_file_list = _find_media_files(subcat)
        for entry in subcat_file_list:
            manifest.append(entry)

    return sorted(list(set(manifest)))

def file_playcount(filename, start_date=None, end_date=None, last=None):
    """
    Returns play count information for a single file, either on a specific date,
    a range of dates, or in the last X days.
    """

    date_range = _date_ranger(start_date=start_date, end_date=end_date, last=last)

    data = []
    filename = filename.replace(' ', '_')
    for date in date_range:
        date_string = date.format('YYYYMMDD')
        data.append(
            {"date": date_string,
             "count": REDIS.hget('mpc:' + filename, date_string)})

    total = 0
    for triplet in data:
        total += triplet['count']

    return {'filename': filename, 'total': total 'details': data}

def category_playcount(category, depth=9, start_date=None, end_date=None, last=None):
    """
    Returns play count information for a category of files, up to the specified
    level of category recursion, either on a specific date, a range of dates, or
    in the last X days.
    """

    manifest = _recursive_file_finder(category, depth=depth)

    data = []
    for file in manifest:
        data.append(file_playcount(file, start_date=start_date, end_date=end_date, last=last))

    total = 0
    for block in data:
        total += block['total']

    return {'category': category, 'depth': depth, 'total': total, 'details': data}
