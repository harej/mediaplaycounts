import arrow, hashlib, re
from .helper import Helper
from collections import OrderedDict

h = Helper()


def _find_subcategories(category, depth=9):
    """
    Finds subcategories of a given category up to the provided depth. Category
    should not have the "Category:" prefix. Returns a flat list of categories.
    """

    if depth == 0:
        return [category]

    categorylist = []

    query = (
        "select page_title from categorylinks join page on cl_from = page_id"
        " where cl_to = %s and cl_type = 'subcat'")
    params = (category.replace(' ', '_'))

    results = h.query_commons(query, params)

    for result in results:
        val = result[0].decode('utf-8')
        categorylist.append(val)
        more = _find_subcategories(val, depth=depth - 1)
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
    results = h.query_commons(q, params)
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
    a range of dates, in the last X days, or all-time by having all the keyword
    parameters set to None.
    """

    data = []
    filename = filename.replace(' ', '_')

    if start_date is None and end_date is None and last is None:
        everything = h.redis.hgetall('mpc:' + filename)
        for date_string, count in everything.items():
            date_string = date_string.decode('utf-8')
            count = int(count.decode('utf-8'))
            data.append({'date': date_string, 'count': count})
    else:
        date_range = h.date_ranger(
            start_date=start_date, end_date=end_date, last=last)
        for date in date_range:
            date_string = date.format('YYYYMMDD')
            count = h.redis.hget('mpc:' + filename, date_string)
            if count is None:
                count = 0
            else:
                count = int(count.decode('utf-8'))
            data.append(OrderedDict([('date', date_string), ('count', count)]))

    total = 0
    for triplet in data:
        total += triplet['count']

    data = sorted(data, key=lambda k: k['date'])

    return OrderedDict([('filename', filename), ('total', total), ('details',
                                                                   data)])


def category_playcount(category,
                       depth=9,
                       start_date=None,
                       end_date=None,
                       last=None):
    """
    Returns play count information for a category of files, up to the specified
    level of category recursion, either on a specific date, a range of dates, or
    in the last X days.
    """

    manifest = _recursive_file_finder(category, depth=depth)

    data = []
    for file in manifest:
        data.append(
            file_playcount(
                file, start_date=start_date, end_date=end_date, last=last))

    data = sorted(data, key=lambda k: k['filename'])

    total = 0
    for block in data:
        total += block['total']

    return OrderedDict([('category', category), ('depth', depth),
                        ('total', total), ('details', data)])


def youtube_snapshot_file(filename, start_date=None, end_date=None, last=None):
    """
    Returns the total plays for a YouTube video, identified by its filename on
    Commons. There is no daily granularity. Rather, the result is the *total*
    number of plays, *as of* the given timestamp.

    Results are only returned if there is a YouTube video paired with the given
    Commons filename.
    """

    filename = filename.replace(' ', '_')
    youtube_id = h.redis.get('com2yt:' + filename)

    if youtube_id is None:
        return {'filename': filename}
    else:
        youtube_id = youtube_id.decode('utf-8')

    play_counts = h.redis.hgetall('youtube:' + youtube_id)
    play_counts = {
        x.decode('utf-8'): int(y.decode('utf-8'))
        for x, y in play_counts.items()
    }
    timestamps = play_counts.keys().sorted(reverse=True)
    latest_time = timestamps[0]
    latest_count = play_counts[latest_time]
    ret = OrderedDict([('filename', filename), ('count', latest_count),
                       ('as_of', latest_time)])

    if start_date is None and end_date is None and last is None:
        return ret
    else:
        details = []
        time_map = {timestamp[:8]: timestamp for timestamp in timestamps}
        date_range = h.date_ranger(
            start_date=start_date, end_date=end_date, last=last)
        for date in date_range:
            date_string = date.format('YYYYMMDD')
            timestamp = time_map[date_string]
            if timestamp in play_counts:
                details.append({
                    'count': play_counts[timestamp],
                    'as_of': timestamp
                })
        ret.update({'details': details})
        return ret


def youtube_snapshot_category(category,
                              depth=9,
                              start_date=None,
                              end_date=None,
                              last=None):
    """
    Does the samne thing as the function above, but with a whole category of
    files.
    """

    manifest = _recursive_file_finder(category, depth=depth)

    data = []
    for file in manifest:
        data.append(
            youtube_snapshot_file(
                file, start_date=start_date, end_date=end_date, last=last))

    data = sorted(data, key=lambda k: k['filename'])

    total = 0
    for block in data:
        total += block['count']

    return OrderedDict([('category', category), ('depth', depth),
                        ('total', total), ('details', data)])


def image_single_viewcount(filename, start_date=None, end_date=None,
                           last=None):
    """
    Returns view count data for static images, including drill-down metrics for
    loads of thumbnails and of the original images.
    """

    metric_groups = ['original', '0-399', '400-799', '800+']

    data = []
    filename = filename.replace(' ', '_')
    filehash = hashlib.sha224(filename).hexdigest()

    if start_date is None and end_date is None and last is None:
        dates = {}
        everything = h.ssdb.hgetall('img:' + filehash)
        for date_string, count in everything.items():
            date_string = date_string.decode('utf-8')
            actual_date = date_string[:8]
            metric_group = date_string[-1:]
            count = int(count.decode('utf-8'))
            if actual_date not in dates:
                dates[actual_date] = {'date': actual_date}
            dates[actual_date][metric_groups[metric_group]] = count
        for date in dates.keys().sorted():
            to_append = OrderedDict([('date', date)])
            total = 0
            for group in metric_groups:
                if group in dates[date]:
                    total += dates[date][group]
                    to_append.update({group: dates[date][group]})
                else:
                    to_append.update({group: 0})
            to_append.update({'total': total})
            data.append(to_append)

    else:
        total = [0, 0, 0, 0]  # corresponding to each metrics group
        date_range = h.date_ranger(
            start_date=start_date, end_date=end_date, last=last)
        for date in date_range:
            date_string = date.format('YYYYMMDD')
            to_append = OrderedDict([('date', date_string)])
            subtotal = 0
            for group_num, group_name in enumerate(metric_groups):
                count = h.ssdb.hget('img:' + filehash,
                                    date_string + str(group_num))
                if count is None:
                    count = 0
                else:
                    count = int(count.decode('utf-8'))
                    subtotal += count
                    total[group_num] += count
                to_append.update({group_name: count})
            to_append.update({'total': subtotal})
            data.append(to_append)

        total_total = total[0] + total[1] + total[2] + total[3]

    data = sorted(data, key=lambda k: k['date'])

    return OrderedDict([('filename', filename), (metric_groups[0], total[0]),
                        (metric_groups[1], total[1]),
                        (metric_groups[2], total[2]),
                        (metric_groups[3], total[3]),
                        ('total', total_total), ('details', data)])


def image_category_viewcount(category,
                             depth=9,
                             start_date=None,
                             end_date=None,
                             last=None):
    """
    Does the samne thing as the function above, but with a whole category of
    files.
    """

    metric_groups = ['original', '0-399', '400-799', '800+', 'total']

    manifest = _recursive_file_finder(category, depth=depth)

    data = []
    for file in manifest:
        data.append(
            image_single_viewcount(
                file, start_date=start_date, end_date=end_date, last=last))

    data = sorted(data, key=lambda k: k['filename'])

    total = [0, 0, 0, 0, 0]
    for block in data:
        for group_num, group_name in enumerate(metric_groups):
            total[group_num] += block[group_name]

    return OrderedDict(
        [('category', category), ('depth', depth),
         (metric_groups[0], total[0]), (metric_groups[1], total[1]),
         (metric_groups[2], total[2]), (metric_groups[3], total[3]),
         (metric_groups[4], total[4]), ('details', data)])
