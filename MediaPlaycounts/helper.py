import arrow, redis, pyssdb, pymysql

try:
    from . import config
except:
    import config


class Helper:
    def __init__(self):
        self.settings = {
            'redis_host': config.REDIS_HOST,
            'redis_port': config.REDIS_PORT,
            'ssdb_host': config.SSDB_HOST,
            'ssdb_port': config.SSDB_PORT,
            'success_log': config.SUCCESS_LOG,
            'error_log': config.ERROR_LOG,
            'commons_host': config.COMMONS_HOST,
            'commons_port': config.COMMONS_PORT,
            'commons_db': config.COMMONS_DB,
            'sql_user': config.SQL_USER,
            'sql_pass': config.SQL_PASS,
            'google_api': config.GOOGLE_API
        }

        self.redis = redis.Redis(
            host=self.settings['redis_host'], port=self.settings['redis_port'])

        self.ssdb = pyssdb.Client(
            host=self.settings['ssdb_host'], port=self.settings['ssdb_port'])

    def success_log(self, message):
        timestamp = arrow.utcnow().format('YYYY-MM-DD HH:mm:ss')
        save_to = self.settings['success_log']
        with open(save_to, "a") as f:
            f.write(timestamp + "\t" + message + "\n")

    def error_log(self, message):
        timestamp = arrow.utcnow().format('YYYY-MM-DD HH:mm:ss')
        save_to = self.settings['error_log']
        with open(save_to, "a") as f:
            f.write(timestamp + "\t" + message + "\n")

    def query_commons(self, query, params):
        """
        Helper function to perform database queries
        """

        conn = pymysql.connect(
            host=self.settings['commons_host'],
            port=self.settings['commons_port'],
            db=self.settings['commons_db'],
            user=self.settings['sql_user'],
            password=self.settings['sql_pass'],
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

    def date_ranger(self, start_date=None, end_date=None, last=None):
        """
        Helper function to take whatever date input the user gives and turn it into
        a range of Arrow objects
        """

        if end_date is None:
            end_date = arrow.utcnow().replace(days=-1)
        else:
            end_date = arrow.get(end_date, 'YYYYMMDD')

        if start_date is not None:
            start_date = arrow.get(start_date, 'YYYYMMDD')
        elif last is not None:
            amount = (last * -1) + 1
            start_date = end_date.replace(days=amount)
        else:
            start_date = end_date

        return arrow.Arrow.range('day', start_date, end_date)
