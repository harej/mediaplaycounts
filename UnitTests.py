import unittest
import arrow
import csv
import pymysql
import random
from MediaPlaycounts import LogProcessor
from MediaPlaycounts.GetData import AskCommons, FilePlaycount

with open("WHEREAMI") as f:
    LOCATION = f.read().strip()
    if LOCATION == "Norepinephrine":
        host = "localhost"
        db = "mediaplaycounts"
        read_default_file = "testfiles/test.cnf"
    elif LOCATION == "ToolLabs":
        host = "tools-db"
        db = "s53189__mediaplaycounts_test"
        read_default_file = "../.my.cnf"
    else:
        raise Exception

def create_table():
    # Opening database connection
    conn = pymysql.connect(host=host,
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

def add_sample_data():
    # Assumes that the above create_table() function has been run.

    conn = pymysql.connect(host=host,
                           port=3306,
                           db=db,
                           read_default_file=read_default_file,
                           charset="utf8")
    cur = conn.cursor()

    q = ("insert into `counts` (`date`, `file`, `viewcount`) "
         "values ('20150228', 'Test_case.ogg', 3), "
         "('20150301', 'Test_case.ogg', 56);")

    cur.execute(q, None)
    conn.commit()
    conn.close()

def add_time_relative_sample_data():
    # Assumes that the above create_table() function has been run.

    conn = pymysql.connect(host=host,
                           port=3306,
                           db=db,
                           read_default_file=read_default_file,
                           charset="utf8")
    cur = conn.cursor()

    value_tuples = [(arrow.utcnow().replace(days=-x).format("YYYYMMDD"),
                     "Relative_test_case.ogg", 13)
                    for x in range(1, 91)]

    q = "insert into `counts` (`date`, `file`, `viewcount`) values "

    for thing in value_tuples:
        q += str(thing) + ", "
    q = q[:-2]

    cur.execute(q, None)
    conn.commit()
    conn.close()

class LogProcessorDownloadTest(unittest.TestCase):
    # This test takes over 7 minutes to run. Use it at your peril.
    def test(self):
        DO_IT = False

        if DO_IT == True:
            try_it_out = LogProcessor.download(arrow.get('2015-01-01'),
                                               success_log="testfiles/success_log.txt",
                                               error_log="testfiles/error_log.txt")

            with open("testfiles/mediacounts.2015-01-01.v00.tsv") as f:
                test_file = f.read()
                self.assertEqual(try_it_out, test_file)
        else:
            self.assertTrue(True)


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
            should_result_in = [("Finally a video.webm", 584023)]
            try_it_out = LogProcessor.parse(log_to_test_on,
                                            success_log="testfiles/success_log.txt",
                                            error_log="testfiles/error_log.txt")
            self.assertEqual(try_it_out, should_result_in)

class LogProcessorStoreBasicTest(unittest.TestCase):
    def test(self):
        date = arrow.get("2015-01-01")
        record = [("Finally a video.webm", 1234),
                  ("Another sort of video.webm", 4567)]

        create_table()
        outcome = LogProcessor.store(record, date, db, read_default_file, host,
                                     success_log="testfiles/success_log.txt",
                                     error_log="testfiles/error_log.txt")
        self.assertTrue(outcome)

class LogProcessorStoreStressTest(unittest.TestCase):
    def test(self):
        date = arrow.get("2015-01-01")
        record = [("A very long public domain opera part {0}.ogg".format(str(x)),
                 random.randrange(1, 999999))
                 for x in range(0, 99999)]

        create_table()
        outcome = LogProcessor.store(record, date, db, read_default_file, host,
                                     success_log="testfiles/success_log.txt",
                                     error_log="testfiles/error_log.txt")
        self.assertTrue(outcome)

class GetDataFilePlaycountDateTest(unittest.TestCase):
    def test(self):
        create_table()
        add_sample_data()

        try_it_out = FilePlaycount.date("Test case.ogg", "20150228",
                     db=db, read_default_file=read_default_file, host=host,
                     success_log="testfiles/success_log.txt",
                     error_log="testfiles/error_log.txt")

        should_result_in = [{"filename": "Test_case.ogg", "date": "20150228",
                            "count": 3}]

        self.assertEqual(try_it_out, should_result_in)

class GetDataFilePlaycountDateRangeTest(unittest.TestCase):
    def test(self):
        create_table()
        add_sample_data()

        try_it_out = FilePlaycount.date_range("Test case.ogg", "20150228", "20150301",
                     db=db, read_default_file=read_default_file, host=host,
                     success_log="testfiles/success_log.txt",
                     error_log="testfiles/error_log.txt")

        should_result_in = [{"filename": "Test_case.ogg", "date": "20150228",
                            "count": 3},
                            {"filename": "Test_case.ogg", "date": "20150301",
                            "count": 56}]

        self.assertEqual(try_it_out, should_result_in)

class GetDataFilePlaycountLast30Test(unittest.TestCase):
    def test(self):
        create_table()
        add_time_relative_sample_data()

        try_it_out = FilePlaycount.last_30("Relative test case.ogg", db=db,
                     read_default_file=read_default_file, host=host,
                     success_log="testfiles/success_log.txt",
                     error_log="testfiles/error_log.txt")

        should_result_in = []

        for x in range(1, 31):
            to_add = {"filename": "Relative_test_case.ogg",
                      "date": arrow.utcnow().replace(days=-x).format("YYYYMMDD"),
                      "count": 13}

            should_result_in.append(to_add)

        self.assertEqual(try_it_out, should_result_in)

class FilePlaycountLast90Test(unittest.TestCase):
    def test(self):
        create_table()
        add_time_relative_sample_data()

        try_it_out = FilePlaycount.last_90("Relative test case.ogg", db=db,
                     read_default_file=read_default_file, host=host,
                     success_log="testfiles/success_log.txt",
                     error_log="testfiles/error_log.txt")

        should_result_in = []

        for x in range(1, 91):
            to_add = {"filename": "Relative_test_case.ogg",
                      "date": arrow.utcnow().replace(days=-x).format("YYYYMMDD"),
                      "count": 13}

            should_result_in.append(to_add)

        self.assertEqual(try_it_out, should_result_in)

class AskCommonsFindSubCategoriesTest(unittest.TestCase):
    def test(self):
        # NOTE: This test should be rewritten to use a "fake Commons". Currently
        # testing with the real Commons to see if the thing even works.

        category = "National Institute for Occupational Safety and Health"

        try_it_out = AskCommons.find_subcategories(category,
                                success_log="testfiles/success_log.txt",
                                error_log="testfiles/error_log.txt")

        should_result_in = ["John_Howard_(public_health_administrator)",
                            "John_Howard_at_WikiConference_USA_2015",
                            "National_Institute_for_Occupational_Safety_and_Health_publications",
                            "National_Institute_for_Occupational_Safety_and_Health_videos",
                            "National_Institute_for_Occupational_Safety_and_Health_sound_recordings",
                            "Power_tool_noise_and_vibration_tests_by_the_National_Institute_for_Occupational_Safety_and_Health"]

        should_result_in.sort()

        self.assertEqual(try_it_out, should_result_in)

class AskCommonsFindMediaFilesTest(unittest.TestCase):
    def test(self):
        # NOTE: This test should be rewritten to use a "fake Commons". Currently
        # testing with the real Commons to see if the thing even works.

        category = "National Institute for Occupational Safety and Health videos"

        try_it_out = AskCommons.find_media_files(category,
                     success_log="testfiles/success_log.txt",
                     error_log="testfiles/error_log.txt")

        should_result_in = ["A_construction_framer_talks_about_protecting_his_crew_from_falls.webm",
                            "Anthrax_surface_sampling_-_How_to_sample_with_cellulose_sponge_on_nonporous_surfaces.webm",
                            "Anthrax_surface_sampling-_How_to_sample_with_macrofoam_swab_on_nonporous_surfaces.webm",
                            "Buy_Quiet_–_For_Manufacturers.webm",
                            "Buy_Quiet_Construction_Video.webm",
                            "Efficacy_of_Face_Shields_Against_Cough_Aerosol_Droplets_from_a_Cough_Simulator.webm",
                            "Cutting_fiber_cement_siding_--_silica_dust_and_lung_disease.webm",
                            "Emergency_Responder_Health_Monitoring_and_Surveillance_Overview.webm",
                            "Hand_Arm_Vibration_Study.webm",
                            "Escape_From_Farmington_No_9_An_Oral_History.webm",
                            "Healthcare_worker_protective_research-_Is_flushing_the_toilet_hazardous-.webm",
                            "How_Poison_Ivy_Works.webm",
                            "Handling_Explosives_in_Underground_Mines.webm",
                            "Know_Your_Nailer-_Nail_Gun_Safety.webm",
                            "Indium_Lung_Disease.webm",
                            "Man_Overboard_Prevention_and_Recovery.webm",
                            "NIOSH_Health_Hazard_Evaluations-_Sampling_for_Exposures.webm",
                            "NIOSH_Nano_Research_-_Engineering_Controls_for_Nanomaterial_Production_and_Handling_Processes.webm",
                            "NIOSH_No_Nose_Saddle_Explained.webm",
                            "Nurses_Voices_(Unit_2)_from_NIOSH_Workplace_Violence_Prevention_for_Nurses_Course.webm",
                            "Occupational_Ladder_Fall_Injuries_—_United_States,_2011.webm",
                            "Move_It_Rig_Move_Safety_for_Roughnecks.webm",
                            "Reducing_Dust_inside_Enclosed_Cabs.webm",
                            "Move_It_Rig_Move_Safety_for_Truckers.webm",
                            "Respirator_Certification_-_As_Vital_as_the_Air_We_Breathe.webm",
                            "Sleep_Deprivation_–_Shift_Work_&_Long_Work_Hours_Put_Nurses_at_Risk.webm",
                            "Rock_Falls_-_Preventing_Rock_Fall_Injuries_in_Underground_Mines.webm",
                            "Take_Pride_in_Your_Job_-_Fall_Protection.webm",
                            "What_it_Means_to_be_NIOSH-Approved_-_A_look_into_N95_Certification_Testing.webm",
                            "Take_Pride_in_Your_Job_-_Seat_Belts.webm",
                            "Zen_and_the_Art_of_Rockbolting.webm"]

        should_result_in.sort()

        self.assertEqual(try_it_out, should_result_in)