import arrow, mwparserfromhell, pywikibot, requests
from helper import Helper
from pprint import pprint

h = Helper()
s = requests.Session()
site = pywikibot.Site()


def _get_manifest():
    timestamp = arrow.utcnow().format('YYYYMMDDHHmmss')

    q = ('select page_title from page join templatelinks on tl_from = page_id '
         'where page_namespace = 6 and tl_namespace = 10 '
         'and tl_title = "From_YouTube";')

    return [x[0].decode('utf-8') for x in h.query_commons(q, None)]


def _get_video_id(file):
    page = pywikibot.Page(site, 'File:' + file)
    parsed = mwparserfromhell.parse(page.text)
    templates = parsed.filter_templates()
    for template in templates:
        if template.name == 'From YouTube':
            return str(template.get(1).value)


def _get_youtube_data(video_id):
    params = {
        'part': 'statistics',
        'id': video_id,
        'key': h.settings['google_api']
    }

    try:
        r = s.get('https://www.googleapis.com/youtube/v3/videos', params=params)
        timestamp = arrow.utcnow().format('YYYYMMDDHHmmss')
        r = r.json()
        return (r['items'][0]['statistics']['viewCount'], timestamp)
    except Exception as e:
        pprint(r)
        h.error_log('YouTube Processor choked on: ' + video_id)
        raise e


def _store_in_redis(video_id, timestamp, view_count):
    h.redis.hset('youtube:' + video_id, timestamp, view_count)


def run():
    manifest = _get_manifest()
    processed = 0
    for file in manifest:
        print('Processing: ' + file)
        try:
            video_id = _get_video_id(file)
            if video_id is not None:
                view_count, timestamp = _get_youtube_data(video_id)
                _store_in_redis(video_id, timestamp, view_count)
                processed += 1
        except Exception as e:
            h.error_log(str(e))
            raise e
        finally:
            log_msg = 'Processed ' + str(processed) + ' YouTube videos'
            h.success_log(log_msg)


if __name__ == '__main__':
    run()
