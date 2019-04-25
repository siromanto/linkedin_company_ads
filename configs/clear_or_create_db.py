#!/usr/bin/env python

from configs import config, helpers


def create_connector():
    client_config = helpers.get_client_config(config.CLIENT_CONFIG_PATH)
    config_db = helpers.get_client_config(config.DB_CONFIG_PATH)

    conn = helpers.establish_db_conn(config_db['user'],
                                     config_db['pwd'],
                                     config_db['account'],
                                     client_config['db'],
                                     client_config['warehouse'])
    return conn


def run(table_name, table_columns):
    conn = create_connector()
    cs = conn.cursor()

    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])

        cs.execute(
            "CREATE OR REPLACE TABLE "
            "{}({})".format(table_name, table_columns))

        print(f'Database {table_name} successfully created or cleared')
    finally:
        cs.close()
    conn.close()


if __name__ == '__main__':
    run(table_name='LINKEDIN_CONSOLE_RAW_DATA', table_columns=helpers.RAW_DB_COLUMNS)
    # run(table_name='TEST_GAAN_V16_TRAFFICBYDAY', table_columns=helpers.V16_DB_COLUMNS)
