# TODO: put some comments here
#
#

import threading
import time
import optparse
import configparser
import mysql.connector

DEBUG = False
STMT_FILE = "pydbflood.sql"
CONFIG_FILE = "pydbflood.cfg"

df_statements = []
df_result = []
df_config = {}


# df_config = {"dbms": "mysql", "host": "172.16.117.101", "port": "3306",
#              "dbname": "SAMPLE", "dbuser": "dbuser", "dbpass": "123456",
#              "parallel": 10, "iteration1": 2, "iteration2": 2, "autocommit": "False"}


def get_mysql_connect():
    global df_config
    try:
        cnx = mysql.connector.connect(
                user=df_config["dbuser"],
                password=df_config["dbpass"],
                host=df_config["host"],
                port=df_config["port"],
                database=df_config["dbname"],
                autocommit=df_config["autocommit"]
        )
    except mysql.connector.Error as err:
        print("Can not connect to database!")
        print(err)
    return cnx


def get_db2_connect():
    # TODO: create db2 connect
    pass


def get_postgresql_connect():
    # TODO: create postgresql connect
    pass


def run_test(thread_test_result):
    """ put comment here
    :type thread_test_result: list
    :param thread_test_result:
    """

    global df_config

    def record_result(test_result, elapsed_time):
        """
        :param test_result:
        :rtype: object
        """
        if elapsed_time < test_result[0]:
            test_result[0] = elapsed_time
        if elapsed_time > test_result[1]:
            test_result[1] = elapsed_time
        test_result[2] += elapsed_time

    tran_result = [10000, 0, 0]
    stmt_result = []
    for num in range(len(df_statements)):
        stmt_result.append([10000, 0, 0])

    if df_config["dbms"] == "mysql":
        get_conn = get_mysql_connect
    elif df_config["dbms"] == "db2":
        get_conn = get_db2_connect
    elif df_config["dbms"] == "postgresql":
        get_conn = get_postgresql_connect
    else:
        print("DBMS {0} is not supported".format(df_config["dbms"]))
        return

    cnx = get_conn()
    cursor = cnx.cursor()
    test_start_time = time.time()

    for iter1 in range(df_config["iteration1"]):
        for iter2 in range(df_config["iteration2"]):
            tran_start_time = time.time()
            for num in range(len(df_statements)):
                try:
                    stmt_start_time = time.time()
                    cursor.execute(df_statements[num])
                    stmt_elapsed_time = time.time() - stmt_start_time
                    record_result(stmt_result[num], stmt_elapsed_time)
                except cursor.execute.Error as err:
                    print(err)
                    cnx.rollback()
                finally:
                    cnx.commit()
            tran_elapsed_time = time.time() - tran_start_time
            record_result(tran_result, tran_elapsed_time)

    test_elapsed_time = time.time() - test_start_time
    cnx.close()
    thread_test_result.append(test_elapsed_time)
    thread_test_result.append(tran_result)
    thread_test_result.append(stmt_result)


def print_test_result():
    """ Print test result """

    # 1. min/max/avg response time for each statement in each thread
    # 2. min/max/avg response time for each statement overall
    # 3. QPS/TPS for each thread
    # 4. Overall QPS/TPS

    global df_result

    print("------ Test result (in seconds) ------\n")

    total_trans_per_thread = df_config["iteration1"] * df_config["iteration2"]
    total_stmts_per_thread = total_trans_per_thread * len(df_statements)
    total_trans = total_trans_per_thread * df_config["parallel"]
    total_stmts = total_trans * len(df_statements)
    summary = 0
    for num in range(df_config["parallel"]):
        summary += df_result[num][0]
    total_elapsed_time = summary / df_config["parallel"]

    print("Total: {0} trans/{1} stmts in {2:.5f} seconds\n".format(total_trans, total_stmts, total_elapsed_time))
    print("Overall TPS: {0:.1f}".format(total_trans / total_elapsed_time))
    print("Overall QPS: {0:.1f}".format(total_stmts / total_elapsed_time))

    min_value = 10000
    for num in range(df_config["parallel"]):
        if min_value > df_result[num][1][0]:
            min_value = df_result[num][1][0]
    print("Min transaction elapsed time: {0:.5f}".format(min_value))

    max_value = 0
    for num in range(df_config["parallel"]):
        if max_value < df_result[num][1][1]:
            max_value = df_result[num][1][1]
    print("Max transaction elapsed time: {0:.5f}".format(max_value))

    summary = 0
    for num in range(df_config["parallel"]):
        summary += df_result[num][1][2]
    print("Avg transaction elapsed time: {0:.5f}".format(summary / total_trans))

    # For each statement, print its min/max/avg time
    print("\nFor each statement:")
    for num in range(len(df_statements)):
        print("\nStatement {0}:".format(num))
        min_value = 10000
        for num1 in range(df_config["parallel"]):
            if min_value > df_result[num1][2][num][0]:
                min_value = df_result[num1][2][num][0]
        print("Min elapsed time: {0:.5f}".format(min_value))

        max_value = 0
        for num1 in range(df_config["parallel"]):
            if max_value < df_result[num1][2][num][1]:
                max_value = df_result[num1][2][num][1]
        print("Max elapsed time: {0:.5f}".format(max_value))

        summary = 0
        for num1 in range(df_config["parallel"]):
            summary += df_result[num1][2][num][2]
        print("Avg elapsed time: {0:.5f}".format(summary / total_trans))

    print("\nFor each thread:\n")
    for num in range(df_config["parallel"]):
        tps = total_trans_per_thread / df_result[num][0]
        qps = total_stmts_per_thread / df_result[num][0]
        print("Thread {0}: TPS={1:.1f}, QPS={2:.1f}".format(num, tps, qps))

    if DEBUG:
        print("total_trans_per_thread: {0}".format(total_trans_per_thread))
        print("total_stmts_per_thread: {0}".format(total_stmts_per_thread))
        print("total_trans: {0}".format(total_trans))
        print("total_stmts: {0}".format(total_stmts))
        print("total_elapsed_time: {0}".format(total_elapsed_time))
        for num in range(df_config["parallel"]):
            print(df_result[num])


def parse_stmt_file(stmt_file):
    """ put comment here
    :param stmt_file:
    """

    global df_statements

    # sql_statement = """
    #       INSERT INTO tb1 VALUES (2,'b');
    #       """
    # df_statements.append(sql_statement)

    for stmt in open(stmt_file):
        if DEBUG:
            print(stmt)
        if stmt.strip() != '':
            df_statements.append(stmt)
    if DEBUG:
        print(df_statements)


def parse_config_file(config_file):
    """ put comment here
    :param config_file:
    """

    global df_config

    config = configparser.ConfigParser()
    config.read(config_file)

    # Parse Basic options
    df_config["parallel"] = int(config.get("Basic", "parallel"))
    df_config["iteration1"] = int(config.get("Basic", "iteration1"))
    df_config["iteration2"] = int(config.get("Basic", "iteration2"))

    # Parse Data Source options
    df_config["dbms"] = config.get("Data Source", "dbms")
    df_config["host"] = config.get("Data Source", "host")
    df_config["port"] = config.get("Data Source", "port")
    df_config["dbname"] = config.get("Data Source", "dbname")
    df_config["dbuser"] = config.get("Data Source", "dbuser")
    df_config["dbpass"] = config.get("Data Source", "dbpass")
    df_config["autocommit"] = config.get("Data Source", "autocommit")

    if DEBUG:
        print(df_config)


def parse_command_options():
    # TODO: parse command line options
    pass


def main():
    """ put comment here """

    # Parse config file
    parse_config_file(CONFIG_FILE)

    # Parse command line options
    parse_command_options()

    # Parse SQL statements file
    parse_stmt_file(STMT_FILE)

    # Create worker threads
    worker_threads = []
    for num in range(df_config["parallel"]):
        df_result.append([])
        worker_threads.append(threading.Thread(target=run_test, args=(df_result[-1],), name=str(num)))

    # Run test
    print("Start testing ...")
    for worker_thread in worker_threads:
        worker_thread.start()

    thread_counter = 1 + df_config["parallel"]
    while thread_counter != 1:
        time.sleep(2)
        thread_counter = len(threading.enumerate())
        print("\rWorking threads: {0}".format(str(thread_counter - 1)), end="")
    print("\nTest finished!\n")

    print_test_result()


if __name__ == "__main__":
    main()
