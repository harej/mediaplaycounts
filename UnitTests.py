import unittest
import arrow
import csv
import pymysql
import random
from MediaPlaycounts import LogProcessor
from MediaPlaycounts.GetData import AskCommons, CategoryPlaycount, FilePlaycount

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
         "`date` varchar(8) collate utf8_bin not null, "
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

        should_result_in = {"total": 59, "details": should_result_in}

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

        should_result_in = {"total": 13*30, "details": should_result_in}

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

        should_result_in = {"total": 13*90, "details": should_result_in}

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

class RecursiveMediaFinderTest(unittest.TestCase):
    def test(self):
        # NOTE: This test should be rewritten to use a "fake Commons". Currently
        # testing with the real Commons to see if the thing even works.

        category = "National Institute for Occupational Safety and Health"

        try_it_out = CategoryPlaycount._recursive_media_finder(
                         category,
                         success_log="testfiles/success_log.txt",
                         error_log="testfiles/error_log.txt")

        should_result_in = ["Black_and_Decker_DR211_unloaded_noise.wav",
                            "Black_and_Decker_DR501_unloaded_noise.wav",
                            "Black_and_Decker_DR601_loaded_noise.wav",
                            "Black_and_Decker_DR601_unloaded_noise.wav",
                            "Black_and_Decker_DS321_loaded_noise.wav",
                            "Black_and_Decker_DS321_unloaded_noise.wav",
                            "Black_and_Decker_FS1300CS_loaded_noise.wav",
                            "Black_and_Decker_FS1300CS_unloaded_noise.wav",
                            "Black_and_Decker_FS350_loaded_noise.wav",
                            "Black_and_Decker_FS350_unloaded_noise.wav",
                            "Black_and_Decker_FS540_loaded_noise.wav",
                            "Black_and_Decker_FS540_unloaded_noise.wav",
                            "Black_and_Decker_FS6000HD_loaded_noise.wav",
                            "Black_and_Decker_FS6000HD_unloaded_noise.wav",
                            "Black_and_Decker_JS600_loaded_noise.wav",
                            "Black_and_Decker_JS600_unloaded_noise.wav",
                            "Black_and_Decker_MS500K_loaded_noise.wav",
                            "Black_and_Decker_MS500K_unloaded_noise.wav",
                            "Black_and_Decker_MS550GB_loaded_noise.wav",
                            "Black_and_Decker_MS550GB_unloaded_noise.wav",
                            "Black_and_Decker_MS700G_loaded_noise.wav",
                            "Black_and_Decker_MS700G_unloaded_noise.wav",
                            "Bosch_11224VSR_loaded_noise.wav",
                            "Bosch_11224VSR_unloaded_noise.wav",
                            "Bosch_11235EVS_loaded_noise.wav",
                            "Bosch_11235EVS_unloaded_noise.wav",
                            "Bosch_11236VS_loaded_noise.wav",
                            "Bosch_11236VS_unloaded_noise.wav",
                            "Bosch_11255VSR_loaded_noise.wav",
                            "Bosch_11255VSR_unloaded_noise.wav",
                            "Bosch_11258VSR_loaded_noise.wav",
                            "Bosch_11258VSR_unloaded_noise.wav",
                            "Bosch_1191VSRK_loaded_noise.wav",
                            "Bosch_1191VSRK_unloaded_noise.wav",
                            "Bosch_1194AVSR_loaded_noise.wav",
                            "Bosch_1194AVSR_unloaded_noise.wav",
                            "Bosch_1199VSR_loaded_noise.wav",
                            "Bosch_1199VSR_unloaded_noise.wav",
                            "Bosch_1295DVS_loaded_noise.wav",
                            "Bosch_1295DVS_unloaded_noise.wav",
                            "Bosch_1347A_loaded_noise.wav",
                            "Bosch_1347A_unloaded_noise.wav",
                            "Bosch_1375A_loaded_noise.wav",
                            "Bosch_1375A_unloaded_noise.wav",
                            "Bosch_1590EVS_loaded_noise.wav",
                            "Bosch_1590EVS_unloaded_noise.wav",
                            "Bosch_1700_loaded_noise.wav",
                            "Bosch_1700_unloaded_noise.wav",
                            "Bosch_1700A_loaded_noise.wav",
                            "Bosch_1700A_unloaded_noise.wav",
                            "Bosch_1752_loaded_noise.wav",
                            "Bosch_1752_unloaded_noise.wav",
                            "Bosch_1752G7_loaded_noise.wav",
                            "Bosch_1752G7_unloaded_noise.wav",
                            "Bosch_1775E_loaded_noise.wav",
                            "Bosch_1775E_unloaded_noise.wav",
                            "Bosch_CS20_loaded_noise.wav",
                            "Bosch_CS20_unloaded_noise.wav",
                            "Bosch_CS5_loaded_noise.wav",
                            "Bosch_CS5_unloaded_noise.wav",
                            "Bosch_RS15_loaded_noise.wav",
                            "Bosch_RS15_unloaded_noise.wav",
                            "Bosch_RS35_loaded_noise.wav",
                            "Bosch_RS35_unloaded_noise.wav",
                            "Bosch_RS5_loaded_noise.wav",
                            "Bosch_RS5_unloaded_noise.wav",
                            "Craftsman_172-10865_loaded_noise.wav",
                            "Craftsman_172-10865_unloaded_noise.wav",
                            "Craftsman_172-171840_loaded_noise.wav",
                            "Craftsman_172-171840_unloaded_noise.wav",
                            "Craftsman_315-265670_loaded_noise.wav",
                            "Craftsman_315-265670_unloaded_noise.wav",
                            "Delta_MS250_loaded_noise.wav",
                            "Delta_MS250_unloaded_noise.wav",
                            "DeWalt_28402K_loaded_noise.wav",
                            "DeWalt_28402K_unloaded_noise.wav",
                            "DeWalt_D25103_loaded_noise.wav",
                            "DeWalt_D25103_unloaded_noise.wav",
                            "DeWalt_D28110_loaded_noise.wav",
                            "DeWalt_D28110_unloaded_noise.wav",
                            "DeWalt_D28115_loaded_noise.wav",
                            "DeWalt_D28115_unloaded_noise.wav",
                            "DeWalt_DW130_unloaded_noise.wav",
                            "DeWalt_DW235G_unloaded_noise.wav",
                            "DeWalt_DW257_loaded_noise.wav",
                            "DeWalt_DW257_unloaded_noise.wav",
                            "DeWalt_DW268_loaded_noise.wav",
                            "DeWalt_DW268_unloaded_noise.wav",
                            "DeWalt_DW272_unloaded_noise.wav",
                            "DeWalt_DW290_loaded_noise.wav",
                            "DeWalt_DW290_unloaded_noise.wav",
                            "DeWalt_DW292_loaded_noise.wav",
                            "DeWalt_DW292_unloaded_noise.wav",
                            "DeWalt_DW304P_loaded_noise.wav",
                            "DeWalt_DW304P_unloaded_noise.wav",
                            "DeWalt_DW308M_loaded_noise.wav",
                            "DeWalt_DW308M_unloaded_noise.wav",
                            "DeWalt_DW309K_loaded_noise.wav",
                            "DeWalt_DW309K_unloaded_noise.wav",
                            "DeWalt_DW310_loaded_noise.wav",
                            "DeWalt_DW310_unloaded_noise.wav",
                            "DeWalt_DW311_loaded_noise.wav",
                            "DeWalt_DW311_unloaded_noise.wav",
                            "DeWalt_DW318_loaded_noise.wav",
                            "DeWalt_DW318_unloaded_noise.wav",
                            "DeWalt_DW364_loaded_noise.wav",
                            "DeWalt_DW364_unloaded_noise.wav",
                            "DeWalt_DW368_loaded_noise.wav",
                            "DeWalt_DW368_unloaded_noise.wav",
                            "DeWalt_DW369_loaded_noise.wav",
                            "DeWalt_DW369_unloaded_noise.wav",
                            "DeWalt_DW378G_loaded_noise.wav",
                            "DeWalt_DW378G_unloaded_noise.wav",
                            "DeWalt_DW384_loaded_noise.wav",
                            "DeWalt_DW384_unloaded_noise.wav",
                            "DeWalt_DW400_loaded_noise.wav",
                            "DeWalt_DW400_unloaded_noise.wav",
                            "DeWalt_DW402_loaded_noise.wav",
                            "DeWalt_DW402_unloaded_noise.wav",
                            "DeWalt_DW411_loaded_noise.wav",
                            "DeWalt_DW411_unloaded_noise.wav",
                            "DeWalt_DW421_loaded_noise.wav",
                            "DeWalt_DW421_unloaded_noise.wav",
                            "DeWalt_DW433K_loaded_noise.wav",
                            "DeWalt_DW433K_unloaded_noise.wav",
                            "DeWalt_DW505_loaded_noise.wav",
                            "DeWalt_DW505_unloaded_noise.wav",
                            "DeWalt_DW706_loaded_noise.wav",
                            "DeWalt_DW706_unloaded_noise.wav",
                            "DeWalt_DW818_loaded_noise.wav",
                            "DeWalt_DW818_unloaded_noise.wav",
                            "Global_Machinery_Company_MS1015AUL_loaded_noise.wav",
                            "Global_Machinery_Company_MS1015AUL_unloaded_noise.wav",
                            "Global_Machinery_Company_RAD45KUL_unloaded_noise.wav",
                            "Hitachi_C10FCE_loaded_noise.wav",
                            "Hitachi_C10FCE_unloaded_noise.wav",
                            "Hitachi_C7SB2_loaded_noise.wav",
                            "Hitachi_C7SB2_unloaded_noise.wav",
                            "Hitachi_CR13V_loaded_noise.wav",
                            "Hitachi_CR13V_unloaded_noise.wav",
                            "Hitachi_D10VH_unloaded_noise.wav",
                            "Hitachi_D13VF_unloaded_noise.wav",
                            "Hitachi_DH24PE_loaded_noise.wav",
                            "Hitachi_DH24PE_unloaded_noise.wav",
                            "Hitachi_FDV16VB2_loaded_noise.wav",
                            "Hitachi_FDV16VB2_unloaded_noise.wav",
                            "Hitachi_G12SE2_loaded_noise.wav",
                            "Hitachi_G12SE2_unloaded_noise.wav",
                            "Hitachi_G12SR2_loaded_noise.wav",
                            "Hitachi_G12SR2_unloaded_noise.wav",
                            "Hitachi_G18MR_loaded_noise.wav",
                            "Hitachi_G18MR_unloaded_noise.wav",
                            "Hitachi_SB-75_loaded_noise.wav",
                            "Hitachi_SB-75_unloaded_noise.wav",
                            "Hitachi_SV12SG_loaded_noise.wav",
                            "Hitachi_SV12SG_unloaded_noise.wav",
                            "Hitachi_W6V3_loaded_noise.wav",
                            "Hitachi_W6V3_unloaded_noise.wav",
                            "Husky_H4103_loaded_noise.wav",
                            "Husky_H4103_unloaded_noise.wav",
                            "Husky_H4140_loaded_noise.wav",
                            "Husky_H4140_unloaded_noise.wav",
                            "Ingersoll-Rand_231G_loaded_noise.wav",
                            "Ingersoll-Rand_231G_unloaded_noise.wav",
                            "Kobalt_loaded_noise.wav",
                            "Kobalt_unloaded_noise.wav",
                            "Makita_4200NH_loaded_noise.wav",
                            "Makita_4200NH_unloaded_noise.wav",
                            "Makita_5007FK_loaded_noise.wav",
                            "Makita_5007FK_unloaded_noise.wav",
                            "Makita_5008NB_loaded_noise.wav",
                            "Makita_5008NB_unloaded_noise.wav",
                            "Makita_5057KB_loaded_noise.wav",
                            "Makita_5057KB_unloaded_noise.wav",
                            "Makita_5277NB_loaded_noise.wav",
                            "Makita_5277NB_unloaded_noise.wav",
                            "Makita_6303H_unloaded_noise.wav",
                            "Makita_6408_unloaded_noise.wav",
                            "Makita_9527NB_loaded_noise.wav",
                            "Makita_9527NB_unloaded_noise.wav",
                            "Makita_9557PB_loaded_noise.wav",
                            "Makita_9557PB_unloaded_noise.wav",
                            "Makita_9910_loaded_noise.wav",
                            "Makita_9910_unloaded_noise.wav",
                            "Makita_B04552_loaded_noise.wav",
                            "Makita_B04552_unloaded_noise.wav",
                            "Makita_GA7021_loaded_noise.wav",
                            "Makita_GA7021_unloaded_noise.wav",
                            "Makita_HP1501_loaded_noise.wav",
                            "Makita_HP1501_unloaded_noise.wav",
                            "Makita_JR3030T_loaded_noise.wav",
                            "Makita_JR3030T_unloaded_noise.wav",
                            "McCulloch_MG832500_loaded_noise.wav",
                            "McCulloch_MG832500_unloaded_noise.wav",
                            "Milwaukee_0299-20_unloaded_noise.wav",
                            "Milwaukee_0300-20_unloaded_noise.wav",
                            "Milwaukee_0302-20_unloaded_noise.wav",
                            "Milwaukee_0375-1_unloaded_noise.wav",
                            "Milwaukee_6148-6_loaded_noise.wav",
                            "Milwaukee_6148-6_unloaded_noise.wav",
                            "Milwaukee_6154-20_loaded_noise.wav",
                            "Milwaukee_6154-20_unloaded_noise.wav",
                            "Milwaukee_6156-20_loaded_noise.wav",
                            "Milwaukee_6156-20_unloaded_noise.wav",
                            "Milwaukee_6266-22_loaded_noise.wav",
                            "Milwaukee_6266-22_unloaded_noise.wav",
                            "Milwaukee_6370-20_loaded_noise.wav",
                            "Milwaukee_6370-20_unloaded_noise.wav",
                            "Milwaukee_6375-20_loaded_noise.wav",
                            "Milwaukee_6375-20_unloaded_noise.wav",
                            "Milwaukee_6378_loaded_noise.wav",
                            "Milwaukee_6378_unloaded_noise.wav",
                            "Milwaukee_6390-20_loaded_noise.wav",
                            "Milwaukee_6390-20_unloaded_noise.wav",
                            "Milwaukee_6391-21_loaded_noise.wav",
                            "Milwaukee_6391-21_unloaded_noise.wav",
                            "Milwaukee_6460_loaded_noise.wav",
                            "Milwaukee_6460_unloaded_noise.wav",
                            "Milwaukee_6509-22_loaded_noise.wav",
                            "Milwaukee_6509-22_unloaded_noise.wav",
                            "Milwaukee_6519-22_loaded_noise.wav",
                            "Milwaukee_6519-22_unloaded_noise.wav",
                            "Milwaukee_6520-21_loaded_noise.wav",
                            "Milwaukee_6520-21_unloaded_noise.wav",
                            "Milwaukee_6521-21_loaded_noise.wav",
                            "Milwaukee_6521-21_unloaded_noise.wav",
                            "Milwaukee_6524-21_loaded_noise.wav",
                            "Milwaukee_6524-21_unloaded_noise.wav",
                            "Milwaukee_6537-22_loaded_noise.wav",
                            "Milwaukee_6537-22_unloaded_noise.wav",
                            "Porter_Cable_314_loaded_noise.wav",
                            "Porter_Cable_314_unloaded_noise.wav",
                            "Porter_Cable_324MAG_loaded_noise.wav",
                            "Porter_Cable_324MAG_unloaded_noise.wav",
                            "Porter_Cable_333_loaded_noise.wav",
                            "Porter_Cable_333_unloaded_noise.wav",
                            "Porter_Cable_340_loaded_noise.wav",
                            "Porter_Cable_340_unloaded_noise.wav",
                            "Porter_Cable_345_loaded_noise.wav",
                            "Porter_Cable_345_unloaded_noise.wav",
                            "Porter_Cable_352VS_loaded_noise.wav",
                            "Porter_Cable_352VS_unloaded_noise.wav",
                            "Porter_Cable_423MAG_loaded_noise.wav",
                            "Porter_Cable_423MAG_unloaded_noise.wav",
                            "Porter_Cable_743_loaded_noise.wav",
                            "Porter_Cable_743_unloaded_noise.wav",
                            "Porter_Cable_7430_loaded_noise.wav",
                            "Porter_Cable_7430_unloaded_noise.wav",
                            "Porter_Cable_850RSOK_loaded_noise.wav",
                            "Porter_Cable_850RSOK_unloaded_noise.wav",
                            "Porter_Cable_9741_loaded_noise.wav",
                            "Porter_Cable_9741_unloaded_noise.wav",
                            "Porter_Cable_9747_loaded_noise.wav",
                            "Porter_Cable_9747_unloaded_noise.wav",
                            "Porter_Cable_9750_loaded_noise.wav",
                            "Porter_Cable_9750_unloaded_noise.wav",
                            "Porter_Cable_PC13CSL_loaded_noise.wav",
                            "Porter_Cable_PC13CSL_unloaded_noise.wav",
                            "Porter_Cable_PC650HD_loaded_noise.wav",
                            "Porter_Cable_PC650HD_unloaded_noise.wav",
                            "Porter_Cable_PC750AG_loaded_noise.wav",
                            "Porter_Cable_PC750AG_unloaded_noise.wav",
                            "Ridgid_R1000_loaded_noise.wav",
                            "Ridgid_R1000_unloaded_noise.wav",
                            "Ridgid_R2500_loaded_noise.wav",
                            "Ridgid_R2500_unloaded_noise.wav",
                            "Ridgid_R2610_loaded_noise.wav",
                            "Ridgid_R2610_unloaded_noise.wav",
                            "Ridgid_R2720_loaded_noise.wav",
                            "Ridgid_R2720_unloaded_noise.wav",
                            "Ridgid_R3000_loaded_noise.wav",
                            "Ridgid_R3000_unloaded_noise.wav",
                            "Ridgid_R3200_loaded_noise.wav",
                            "Ridgid_R3200_unloaded_noise.wav",
                            "Ridgid_R6300_loaded_noise.wav",
                            "Ridgid_R6300_unloaded_noise.wav",
                            "Ryobi_AG401_loaded_noise.wav",
                            "Ryobi_AG401_unloaded_noise.wav",
                            "Ryobi_AG451_loaded_noise.wav",
                            "Ryobi_AG451_unloaded_noise.wav",
                            "Ryobi_BE321VS_loaded_noise.wav",
                            "Ryobi_BE321VS_unloaded_noise.wav",
                            "Ryobi_CFS1501_loaded_noise.wav",
                            "Ryobi_CFS1501_unloaded_noise.wav",
                            "Ryobi_CSB121_loaded_noise.wav",
                            "Ryobi_CSB121_unloaded_noise.wav",
                            "Ryobi_RJ161V_loaded_noise.wav",
                            "Ryobi_RJ161V_unloaded_noise.wav",
                            "Ryobi_RS2418_loaded_noise.wav",
                            "Ryobi_RS2418_unloaded_noise.wav",
                            "Ryobi_RS280VS_loaded_noise.wav",
                            "Ryobi_RS280VS_unloaded_noise.wav",
                            "Skil_4380_loaded_noise.wav",
                            "Skil_4380_unloaded_noise.wav",
                            "Skil_5380-01_loaded_noise.wav",
                            "Skil_5380-01_unloaded_noise.wav",
                            "Skil_5400_loaded_noise.wav",
                            "Skil_5400_unloaded_noise.wav",
                            "Skil_5480-01_loaded_noise.wav",
                            "Skil_5480-01_unloaded_noise.wav",
                            "Skil_5500_loaded_noise.wav",
                            "Skil_5500_unloaded_noise.wav",
                            "Skil_5600_loaded_noise.wav",
                            "Skil_5600_unloaded_noise.wav",
                            "Skil_5680_loaded_noise.wav",
                            "Skil_5680_unloaded_noise.wav",
                            "Skil_5750_loaded_noise.wav",
                            "Skil_5750_unloaded_noise.wav",
                            "Skil_6265_unloaded_noise.wav",
                            "Skil_9290-01_loaded_noise.wav",
                            "Skil_9290-01_unloaded_noise.wav",
                            "Skil_SHD77_loaded_noise.wav",
                            "Skil_SHD77_unloaded_noise.wav",
                            "Tradesman_M2501W_loaded_noise.wav",
                            "Tradesman_M2501W_unloaded_noise.wav",
                            "Tradesman_M3052LW_loaded_noise.wav",
                            "Tradesman_M3052LW_unloaded_noise.wav",
                            "A_construction_framer_talks_about_protecting_his_crew_from_falls.webm",
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
                            "Indium_Lung_Disease.ogv",
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
                            "Zen_and_the_Art_of_Rockbolting.webm",
                            "Special_Remarks_by_Dr._John_Howard_at_WikiConference_USA_2015.webm"]

        should_result_in.sort()

        self.assertEqual(try_it_out, should_result_in)
