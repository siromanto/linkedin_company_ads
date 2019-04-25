# -*- coding: utf-8 -*-

import os

from configs import config, helpers
# from LinkedinAds.configs import config, helpers


def _execute_queries_for_upload(curr, report_path, storage_path, table_name):
    curr.execute('PUT \'file://{}\' \'{}\''.format(report_path, storage_path))
    curr.execute('COPY INTO {} FROM \'{}\' '
                               'FILE_FORMAT=(SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY=\'"\')'.format(table_name,
                                                                                                       storage_path))
    curr.execute('REMOVE \'{}\''.format(storage_path))


def load_data():
    report_path = config.DATA_PATH
    load_raw_data_from_csv(report_path)


def load_daily(**kwargs):
    report_path = config.DATA_PATH
    load_raw_data_from_csv(report_path)


def load_raw_data_from_csv(file_path):
    file_name = file_path.rsplit('/', 1)[-1]

    client_config = helpers.get_client_config(config.CLIENT_CONFIG_PATH)
    db_config = helpers.get_client_config(config.DB_CONFIG_PATH)

    conn = helpers.establish_db_conn(
        db_config['user'],
        db_config['pwd'],
        db_config['account'],
        client_config['db'],
        client_config['warehouse']
    )

    curr = conn.cursor()
    # table_name = client_config['raw_db_table'] # TODO: Fix this
    table_name = client_config['raw_db_table1']

    storage_path = '@%{}/{}'.format(table_name, file_name)
    try:
        curr.execute('BEGIN')
        # _cleanup_data(curr, table_name)
        _execute_queries_for_upload(curr, file_path, storage_path, table_name)
        curr.execute('COMMIT')

        print('FINISH FILE LOADING...')

    except Exception as e:
        print(e)
    finally:
        conn.cursor().close()
        conn.close()
        print("Data imported successfully")
    # os.remove(file_path)


if __name__ == '__main__':
    load_daily()
