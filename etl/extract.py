# -*- coding: utf-8 -*-

import re
from datetime import *
import pytz

from configs import config, helpers
from linkedin.api import LinkedinAdsApi

COMPANY_URN = '18067731'

def prepare_api():
    credentials = helpers.get_client_config(conf_path=config.CLIENT_CONFIG_PATH)
    access_token = credentials.get("access_token")
    access_headers = {'Authorization': 'Bearer {}'.format(access_token)}

    return LinkedinAdsApi(headers=access_headers)


def join_params_to_uri(params):
    return "&".join("".join("{}={}".format(k, v) for k, v in t.items()) for t in params)


def per_delta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


def extract_daily(**kwargs):
    dates = [s.strftime('%Y-%m-%d') for s in per_delta(
        date.today() - timedelta(days=helpers.DAYLOAD),
        date.today(),
        timedelta(days=1)
    )]
    start_date, end_date = dates[0], dates[-1]

    extract_data(start_date, end_date)


def parse_date_to_timestump(str_date):
    t = datetime.strptime(str_date, '%Y-%m-%d')
    utc_time = t.replace(tzinfo=pytz.utc)
    return int(utc_time.timestamp() * 1000)  # timestamp is in milliseconds

def parse_timestump_to_date(ts):
    clear_ts = ts / 1000
    return datetime.utcfromtimestamp(clear_ts).strftime('%Y-%m-%d')


def extract_data(start_date='2018-04-01', end_date=(date.today() - timedelta(1)).strftime('%Y-%m-%d')):  # TODO: Change it to get chunks

    start_date_timestump = parse_date_to_timestump(start_date)
    end_date_timestump = parse_date_to_timestump(end_date)
    daily_data = get_data_from_response(COMPANY_URN, start_date_timestump, end_date_timestump)

    print('DATA COLUMNS --- {}'.format(len(daily_data)))

    with open(config.DATA_PATH, mode='w', encoding='utf8') as raw_csv:
        writer = helpers.prepare_header_for_clear_csv(raw_csv, helpers.CSV_COLUMNS)

        for item in daily_data:
            row = {}
            row.update({
                'DATE': parse_timestump_to_date(item['timeRange']['start']),
                'organization_id': item.get('organizationalEntity'),
                'shareCount': item['totalShareStatistics'].get('shareCount'),
                'uniqueImpressionsCount': item['totalShareStatistics'].get('uniqueImpressionsCount'),
                'clickCount': item['totalShareStatistics'].get('clickCount'),
                'engagement': item['totalShareStatistics'].get('engagement'),
                'likeCount': item['totalShareStatistics'].get('likeCount'),
                'impressionCount': item['totalShareStatistics'].get('impressionCount'),
                'commentCount': item['totalShareStatistics'].get('commentCount')
            })

            writer.writerow(row)
        print('*' * 200)




def get_data_from_response(campaign, start_date_timestump, end_date_timestump):
    api = prepare_api()
    uri_params = [
        {'q': 'organizationalEntity'},
        {'organizationalEntity': 'urn:li:organization:{}'.format(campaign)},
        {'timeIntervals.timeGranularityType': 'DAY'},
        {'timeIntervals.timeRange.start': start_date_timestump},
        {'timeIntervals.timeRange.end': end_date_timestump}
    ]

    data = api.organizationalEntityShareStatistics(params=join_params_to_uri(uri_params)).get("elements")
    return data


if __name__ == '__main__':
    extract_data()
    # extract_data(start_date='2017-01-04', end_date='2019-04-01')
    # extract_daily()