import unittest
import arrow
import csv
import pymysql
import random
from MediaPlaycounts import LogProcessor

def create_table():
    db = "s53189__mediaplaycounts_test"
    read_default_file = "../.my.cnf"

    # Opening database connection
    conn = pymysql.connect(host="tools-db",
                           port=3306,
                           db=db,
                           read_default_file=read_default_file,
                           charset="utf8")
    cur = conn.cursor()

    # Delete the table from prior tests (if it exists)
    delete_query = "drop table if exists `counts`;"
    cur.execute(delete_query, None)
    conn.commit()

    # Create the database we are testing in
    create_db_query = \
        ("create table `counts` "
         "( `id` int(11) not null auto_increment primary key, "
         "`date` varchar(255) collate utf8_bin not null, "
         "`file` varchar(255) collate utf8_bin not null, "
         "`viewcount` int(11) not null ) "
        "engine=InnoDB default charset=utf8 collate=utf8_bin;")

    cur.execute(create_db_query, None)
    conn.commit()
    conn.close()

#class LogProcessorDownloadTest(unittest.TestCase):
#    # This test takes over 7 minutes to run. Use it at your peril.
#    def test(self):
#        try_it_out = LogProcessor.download(arrow.get('2015-01-01'))
#
#        with open("testfiles/mediacounts.2015-01-01.v00.tsv") as f:
#            test_file = f.read()
#            self.assertEqual(try_it_out, test_file)


class LogProcessorParseTest(unittest.TestCase):
    def test(self):
        # Actual log entries are like 25 columns long, but we only work with 0,
        # 3, 4, and 16 so pardon me for abbreviating.

        X = ""

        log_to_test_on = [
        ["/wikipedia/commons/0/00/Not+a+video.jpg", X, X, "11111111", "22222",
        X, X, X, X, X, X, X, X, X, X, X, "4444"],
        ["/math/0/0/0/abcdefghijklmnopqrstuvwxyz.png", X, X, "22222222", "555",
        X, X, X, X, X, X, X, X, X, X, X, "4444"],
        ["/wikipedia/commons/a/bc/Finally+a+video.webm", X, X, "578345", "1234",
        X, X, X, X, X, X, X, X, X, X, X, "4444"]
        ]

        with open("testfiles/testfile.tsv", "w") as f:
            writer = csv.writer(f, delimiter="\t")
            for row in log_to_test_on:
                writer.writerow(row)

        with open("testfiles/testfile.tsv") as f:
            log_to_test_on = f.read()
            should_result_in = [("Finally a video.webm", 578345+1234+4444)]
            self.assertEqual(LogProcessor.parse(log_to_test_on), should_result_in)

class LogProcessorStoreBasicTest(unittest.TestCase):
    def test(self):
        date = arrow.get("2015-01-01")
        record = [("Finally a video.webm", 1234),
                  ("Another sort of video.webm", 4567)]

        create_table()
        outcome = LogProcessor.store(record, date,
                  "s53189__mediaplaycounts_test", "../.my.cnf", host="tools-db")
        self.assertTrue(outcome)

class LogProcessorStoreStressTest(unittest.TestCase):
    def test(self):
        date = arrow.get("2015-01-01")
        record = [("A very long public domain opera part {0}.ogg".format(str(x)),
                 random.randrange(1, 999999))
                 for x in range(0, 99999)]

        create_table()
        outcome = LogProcessor.store(record, date,
                  "s53189__mediaplaycounts_test", "../.my.cnf", host="tools-db")
        self.assertTrue(outcome)
