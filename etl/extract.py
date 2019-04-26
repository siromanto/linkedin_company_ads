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

    follower_data = get_data_from_response1(COMPANY_URN, start_date_timestump, end_date_timestump)

    print('DATA COLUMNS --- {}'.format(len(daily_data)))

    with open(config.DATA_PATH, mode='w', encoding='utf8') as raw_csv:
        writer = helpers.prepare_header_for_clear_csv(raw_csv, helpers.CSV_COLUMNS1)

        for item, follow in zip(daily_data, follower_data):
            row = {}
            row.update({
                'DATE': parse_timestump_to_date(item['timeRange']['start']),
                'organization_id': item.get('organizationalEntity'),
                'shareCount': item['totalShareStatistics'].get('shareCount', 0),
                'uniqueImpressionsCount': item['totalShareStatistics'].get('uniqueImpressionsCount', 0),
                'clickCount': item['totalShareStatistics'].get('clickCount', 0),
                'engagement': item['totalShareStatistics'].get('engagement', 0),
                'likeCount': item['totalShareStatistics'].get('likeCount', 0),
                'impressionCount': item['totalShareStatistics'].get('impressionCount', 0),
                'commentCount': item['totalShareStatistics'].get('commentCount', 0),
                'paidFollowerGain': follow["followerGains"].get('paidFollowerGain'),
                'organicFollowerGain': follow["followerGains"].get('organicFollowerGain')
            })

            writer.writerow(row)
        print('*' * 200)


def extract_page_statistics(start_date='2018-04-01', end_date=(date.today() - timedelta(1)).strftime('%Y-%m-%d')):  # TODO: Change it to get chunks

    start_date_timestump = parse_date_to_timestump(start_date)
    end_date_timestump = parse_date_to_timestump(end_date)
    daily_data = get_data_from_response2(COMPANY_URN, start_date_timestump, end_date_timestump)

    print('DATA COLUMNS --- {}'.format(len(daily_data)))

    with open(config.DATA_PATH2, mode='w', encoding='utf8') as raw_csv:
        writer = helpers.prepare_header_for_clear_csv(raw_csv, helpers.CSV_COLUMNS2)

        for item in daily_data:
            row = {}
            row.update({
                'DATE': parse_timestump_to_date(item['timeRange']['start']),
                'mobileCustomButtonClickCounts': item['totalPageStatistics']['clicks']['mobileCustomButtonClickCounts'][0]['clicks'],
                'careersPageBannerPromoClicks': item['totalPageStatistics']['clicks']['careersPageClicks']['careersPageBannerPromoClicks'],
                'careersPageEmployeesClicks': item['totalPageStatistics']['clicks']['careersPageClicks']['careersPageEmployeesClicks'],
                'careersPagePromoLinksClicks': item['totalPageStatistics']['clicks']['careersPageClicks']['careersPagePromoLinksClicks'],
                'careersPageJobsClicks': item['totalPageStatistics']['clicks']['careersPageClicks']['careersPageJobsClicks'],
                'mobileCareersPageEmployeesClicks': item['totalPageStatistics']['clicks']['mobileCareersPageClicks']['careersPageEmployeesClicks'],
                'mobileCareersPagePromoLinksClicks': item['totalPageStatistics']['clicks']['mobileCareersPageClicks']['careersPagePromoLinksClicks'],
                'mobileCareersPageJobsClicks': item['totalPageStatistics']['clicks']['mobileCareersPageClicks']['careersPageJobsClicks'],
                'desktopCustomButtonClickCounts': item['totalPageStatistics']['clicks']['desktopCustomButtonClickCounts'][0]['clicks'],

                'mobileJobsPageViews': item['totalPageStatistics']['views']['mobileJobsPageViews']['pageViews'],
                'uniqueMobileJobsPageViews': item['totalPageStatistics']['views']['mobileJobsPageViews']['uniquePageViews'],
                'careersPageViews': item['totalPageStatistics']['views']['careersPageViews']['pageViews'],
                'uniqueCareersPageViews': item['totalPageStatistics']['views']['careersPageViews']['uniquePageViews'],
                'mobileLifeAtPageViews': item['totalPageStatistics']['views']['mobileLifeAtPageViews']['pageViews'],
                'uniqueMobileLifeAtPageViews': item['totalPageStatistics']['views']['mobileLifeAtPageViews']['uniquePageViews'],
                'insightsPageViews': item['totalPageStatistics']['views']['insightsPageViews']['pageViews'],
                'uniqueInsightsPageViews': item['totalPageStatistics']['views']['insightsPageViews']['uniquePageViews'],
                'allDesktopPageViews': item['totalPageStatistics']['views']['allDesktopPageViews']['pageViews'],
                'uniqueAllDesktopPageViews': item['totalPageStatistics']['views']['allDesktopPageViews']['uniquePageViews'],
                'mobileAboutPageViews': item['totalPageStatistics']['views']['mobileAboutPageViews']['pageViews'],
                'uniqueMobileAboutPageViews': item['totalPageStatistics']['views']['mobileAboutPageViews']['uniquePageViews'],
                'allMobilePageViews': item['totalPageStatistics']['views']['allMobilePageViews']['pageViews'],
                'uniqueAllMobilePageViews': item['totalPageStatistics']['views']['allMobilePageViews']['uniquePageViews'],
                'desktopJobsPageViews': item['totalPageStatistics']['views']['desktopJobsPageViews']['pageViews'],
                'uniqueDesktopJobsPageViews': item['totalPageStatistics']['views']['desktopJobsPageViews']['uniquePageViews'],
                'jobsPageViews': item['totalPageStatistics']['views']['jobsPageViews']['pageViews'],
                'uniqueJobsPageViews': item['totalPageStatistics']['views']['jobsPageViews']['uniquePageViews'],
                'peoplePageViews': item['totalPageStatistics']['views']['peoplePageViews']['pageViews'],
                'uniquePeoplePageViews': item['totalPageStatistics']['views']['peoplePageViews']['uniquePageViews'],
                'desktopPeoplePageViews': item['totalPageStatistics']['views']['desktopPeoplePageViews']['pageViews'],
                'uniqueDesktopPeoplePageViews': item['totalPageStatistics']['views']['desktopPeoplePageViews']['uniquePageViews'],
                'aboutPageViews': item['totalPageStatistics']['views']['aboutPageViews']['pageViews'],
                'uniqueAboutPageViews': item['totalPageStatistics']['views']['aboutPageViews']['uniquePageViews'],
                'desktopAboutPageViews': item['totalPageStatistics']['views']['desktopAboutPageViews']['pageViews'],
                'uniqueDesktopAboutPageViews': item['totalPageStatistics']['views']['desktopAboutPageViews']['uniquePageViews'],
                'overviewPageViews': item['totalPageStatistics']['views']['overviewPageViews']['pageViews'],
                'uniqueOverviewPageViews': item['totalPageStatistics']['views']['overviewPageViews']['uniquePageViews'],
                'mobilePeoplePageViews': item['totalPageStatistics']['views']['mobilePeoplePageViews']['pageViews'],
                'uniqueMobilePeoplePageViews': item['totalPageStatistics']['views']['mobilePeoplePageViews']['uniquePageViews'],
                'desktopInsightsPageViews': item['totalPageStatistics']['views']['desktopInsightsPageViews']['pageViews'],
                'uniqueDesktopInsightsPageViews': item['totalPageStatistics']['views']['desktopInsightsPageViews']['uniquePageViews'],
                'desktopCareersPageViews': item['totalPageStatistics']['views']['desktopCareersPageViews']['pageViews'],
                'uniqueDesktopCareersPageViews': item['totalPageStatistics']['views']['desktopCareersPageViews']['uniquePageViews'],
                'mobileOverviewPageViews': item['totalPageStatistics']['views']['mobileOverviewPageViews']['pageViews'],
                'uniqueMobileOverviewPageViews': item['totalPageStatistics']['views']['mobileOverviewPageViews']['uniquePageViews'],
                'lifeAtPageViews': item['totalPageStatistics']['views']['lifeAtPageViews']['pageViews'],
                'uniqueLifeAtPageViews': item['totalPageStatistics']['views']['lifeAtPageViews']['uniquePageViews'],
                'desktopOverviewPageViews': item['totalPageStatistics']['views']['desktopOverviewPageViews']['pageViews'],
                'uniqueDesktopOverviewPageViews': item['totalPageStatistics']['views']['desktopOverviewPageViews']['uniquePageViews'],
                'desktopLifeAtPageViews': item['totalPageStatistics']['views']['desktopLifeAtPageViews']['pageViews'],
                'uniqueDesktopLifeAtPageViews': item['totalPageStatistics']['views']['desktopLifeAtPageViews']['uniquePageViews'],
                'mobileCareersPageViews': item['totalPageStatistics']['views']['mobileCareersPageViews']['pageViews'],
                'uniqueMobileCareersPageViews': item['totalPageStatistics']['views']['mobileCareersPageViews']['uniquePageViews'],
                'allPageViews': item['totalPageStatistics']['views']['allPageViews']['pageViews'],
                'uniqueAllPageViews': item['totalPageStatistics']['views']['allPageViews']['uniquePageViews'],
                'mobileInsightsPageViews': item['totalPageStatistics']['views']['mobileInsightsPageViews']['pageViews'],
                'uniqueMobileInsightsPageViews': item['totalPageStatistics']['views']['mobileInsightsPageViews']['uniquePageViews']
            })

            writer.writerow(row)
            print('DATE --- {}'.format(parse_timestump_to_date(item['timeRange']['start'])))
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


def get_data_from_response1(campaign, start_date_timestump, end_date_timestump):
    api = prepare_api()
    uri_params = [
        {'q': 'organizationalEntity'},
        {'organizationalEntity': 'urn:li:organization:{}'.format(campaign)},
        {'timeIntervals.timeGranularityType': 'DAY'},
        {'timeIntervals.timeRange.start': start_date_timestump},
        {'timeIntervals.timeRange.end': end_date_timestump}
    ]

    data = api.organizationalEntityFollowerStatistics(params=join_params_to_uri(uri_params)).get("elements")
    return data


def get_data_from_response2(campaign, start_date_timestump, end_date_timestump):
    api = prepare_api()
    uri_params = [
        {'q': 'organization'},
        {'organization': 'urn:li:organization:{}'.format(campaign)},
        {'timeIntervals.timeGranularityType': 'DAY'},
        {'timeIntervals.timeRange.start': start_date_timestump},
        {'timeIntervals.timeRange.end': end_date_timestump}
    ]

    data = api.organizationPageStatistics(params=join_params_to_uri(uri_params)).get("elements")
    return data


if __name__ == '__main__':
    # extract_data()
    extract_page_statistics()
    # extract_data(start_date='2017-01-04', end_date='2019-04-01')
    # extract_daily()