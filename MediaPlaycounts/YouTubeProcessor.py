import arrow, mwparserfromhell, pywikibot, requests, redis, GetData, WorkLogger, config

REDIS = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)
success_log = config.SUCCESS_LOG
error_log = config.ERROR_LOG
site = pywikibot.Site()
s = requests.Session()


def _get_manifest():
    timestamp = arrow.utcnow().format('YYYYMMDDHHmmss')

    q = ('select page_title from page join templatelinks on tl_from = page_id '
         'where page_namespace = 6 and tl_namespace = 10 '
         'and tl_title = "From_YouTube";')

    return [x[0].decode('utf-8') for x in GetData._query_commons(q, None)]


def _get_video_id(file):
    page = pywikibot.Page(site, 'File:' + file)
    parsed = mwparserfromhell.parse(page.text)
    templates = parsed.filter_templates()
    for template in templates:
        if template.name == 'From YouTube':
            return template.get(1).value


def _get_youtube_data(video_id):
    params = {'part': 'statistics', 'id': video_id, 'key': config.GOOGLE_API}

    r = s.get('https://www.googleapis.com/youtube/v3/videos', params=params)
    timestamp = arrow.utcnow().format('YYYYMMDDHHmmss')
    r = r.json()
    return (r['items'][0]['statistics']['viewCount'], timestamp)


def _store_in_redis(video_id, timestamp, view_count):
    REDIS.hset('youtube:' + video_id, timestamp, view_count)


def run():
    manifest = _get_manifest()
    processed = 0
    for file in manifest:
        print('Processing: ' + file)
        try:
            video_id = _get_video_id(file)
            view_count, timestamp = _get_youtube_data(video_id)
            _store_in_redis(video_id, timestamp, view_count)
            processed += 1
        except Exception as e:
            WorkLogger.error_log(str(e), error_log)
            raise e
        finally:
            log_msg = 'Processed ' + str(processed) + ' YouTube videos'
            WorkLogger.success_log(log_msg, success_log)


if __name__ == '__main__':
    run()
