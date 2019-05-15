# -*- coding: utf-8 -*-

import re
from datetime import *
import pytz

from configs import config, helpers
from linkedin.api import LinkedinAdsApi

COMPANY_URN = '18067731'
TIME_DELAY = 2


def prepare_api():
    credentials = helpers.get_client_config(conf_path=config.CLIENT_CONFIG_PATH)
    access_token = credentials.get("access_token")
    access_headers = {'Authorization': 'Bearer {}'.format(access_token)}
    return LinkedinAdsApi(headers=access_headers)


def join_params_to_uri(params):  # TODO: move into helpers
    return "&".join("".join("{}={}".format(k, v) for k, v in t.items()) for t in params)


def per_delta(start, end, delta):  # TODO: move into helpers
    curr = start
    while curr <= end:
        yield curr
        curr += delta


def parse_date_to_timestump(str_date):  # TODO: move into helpers
    t = datetime.strptime(str_date, '%Y-%m-%d')
    utc_time = t.replace(tzinfo=pytz.utc)
    return int(utc_time.timestamp() * 1000)  # timestamp is in milliseconds


def parse_timestump_to_date(ts):  # TODO: move into helpers
    clear_ts = ts / 1000
    return datetime.utcfromtimestamp(clear_ts).strftime('%Y-%m-%d')


def extract_daily_share_statistic(**kwargs):
    dates = [s.strftime('%Y-%m-%d') for s in per_delta(
        date.today() - timedelta(days=TIME_DELAY),
        date.today() - timedelta(days=TIME_DELAY) + timedelta(days=helpers.DAYLOAD),
        timedelta(days=1)
    )]
    start_date, end_date = dates[0], dates[-1]

    extract_share_data(start_date, end_date)


def extract_share_data(start_date='2018-04-01', end_date=(date.today() - timedelta(3)).strftime('%Y-%m-%d')):
    start_date_timestump = parse_date_to_timestump(start_date)
    end_date_timestump = parse_date_to_timestump(end_date)

    share_data = get_share_statistic_from_response(COMPANY_URN, start_date_timestump, end_date_timestump)
    follower_data = get_follower_statistic_from_response(COMPANY_URN, start_date_timestump, end_date_timestump)

    print('DATA COLUMNS --- {}'.format(len(share_data)))

    with open(config.SHARE_DATA_PATH, mode='w', encoding='utf8') as raw_csv:

        writer = helpers.prepare_header_for_clear_csv(raw_csv, helpers.CSV_COLUMNS1)
        for item, follow in zip(share_data, follower_data):
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
            print('DATE --- {}'.format(parse_timestump_to_date(item['timeRange']['start'])))
        print('*' * 200)


def extract_daily_page_statistic(**kwargs):
    dates = [s.strftime('%Y-%m-%d') for s in per_delta(
        date.today() - timedelta(days=TIME_DELAY),
        date.today() - timedelta(days=TIME_DELAY) + timedelta(days=helpers.DAYLOAD),
        timedelta(days=1)
    )]
    start_date, end_date = dates[0], dates[-1]

    extract_page_statistics(start_date, end_date)


def extract_page_statistics(start_date='2018-04-01', end_date=(date.today() - timedelta(1)).strftime('%Y-%m-%d')):  # TODO: Change it to get chunks

    start_date_timestump = parse_date_to_timestump(start_date)
    end_date_timestump = parse_date_to_timestump(end_date)
    daily_data = get_page_statistic_data_from_response(COMPANY_URN, start_date_timestump, end_date_timestump)

    print('DATA COLUMNS --- {}'.format(len(daily_data)))

    with open(config.PAGE_DATA_PATH, mode='w', encoding='utf8') as raw_csv:
        writer = helpers.prepare_header_for_clear_csv(raw_csv, helpers.CSV_COLUMNS2)

        for item in daily_data:
            row = {}
            row.update({
                'DATE': parse_timestump_to_date(item['timeRange']['start']),
                'mobile_Custom_Button_Click_Counts': item['totalPageStatistics']['clicks']['mobileCustomButtonClickCounts'][0]['clicks'] if item['totalPageStatistics']['clicks']['mobileCustomButtonClickCounts'] else 0,
                'careers_Page_Banner_Promo_Clicks': item['totalPageStatistics']['clicks']['careersPageClicks']['careersPageBannerPromoClicks'],
                'careers_Page_Employees_Clicks': item['totalPageStatistics']['clicks']['careersPageClicks']['careersPageEmployeesClicks'],
                'careers_Page_Promo_Links_Clicks': item['totalPageStatistics']['clicks']['careersPageClicks']['careersPagePromoLinksClicks'],
                'careers_Page_Jobs_Clicks': item['totalPageStatistics']['clicks']['careersPageClicks']['careersPageJobsClicks'],
                'mobile_Careers_Page_Employees_Clicks': item['totalPageStatistics']['clicks']['mobileCareersPageClicks']['careersPageEmployeesClicks'],
                'mobile_Careers_Page_Promo_Links_Clicks': item['totalPageStatistics']['clicks']['mobileCareersPageClicks']['careersPagePromoLinksClicks'],
                'mobile_Careers_Page_Jobs_Clicks': item['totalPageStatistics']['clicks']['mobileCareersPageClicks']['careersPageJobsClicks'],
                'desktop_Custom_Button_Click_Counts': item['totalPageStatistics']['clicks']['desktopCustomButtonClickCounts'][0]['clicks'] if item['totalPageStatistics']['clicks']['desktopCustomButtonClickCounts'] else 0,

                'mobile_Jobs_Page_Views': item['totalPageStatistics']['views']['mobileJobsPageViews']['pageViews'],
                'unique_Mobile_Jobs_Page_Views': item['totalPageStatistics']['views']['mobileJobsPageViews']['uniquePageViews'],
                'careers_Page_Views': item['totalPageStatistics']['views']['careersPageViews']['pageViews'],
                'unique_Careers_Page_Views': item['totalPageStatistics']['views']['careersPageViews']['uniquePageViews'],
                'mobile_Life_At_Page_Views': item['totalPageStatistics']['views']['mobileLifeAtPageViews']['pageViews'],
                'unique_Mobile_Life_At_Page_Views': item['totalPageStatistics']['views']['mobileLifeAtPageViews']['uniquePageViews'],
                'insights_Page_Views': item['totalPageStatistics']['views']['insightsPageViews']['pageViews'],
                'unique_Insights_Page_Views': item['totalPageStatistics']['views']['insightsPageViews']['uniquePageViews'],
                'all_Desktop_Page_Views': item['totalPageStatistics']['views']['allDesktopPageViews']['pageViews'],
                'unique_All_Desktop_Page_Views': item['totalPageStatistics']['views']['allDesktopPageViews']['uniquePageViews'],
                'mobile_About_Page_Views': item['totalPageStatistics']['views']['mobileAboutPageViews']['pageViews'],
                'unique_Mobile_About_Page_Views': item['totalPageStatistics']['views']['mobileAboutPageViews']['uniquePageViews'],
                'all_Mobile_Page_Views': item['totalPageStatistics']['views']['allMobilePageViews']['pageViews'],
                'unique_All_Mobile_Page_Views': item['totalPageStatistics']['views']['allMobilePageViews']['uniquePageViews'],
                'desktop_Jobs_Page_Views': item['totalPageStatistics']['views']['desktopJobsPageViews']['pageViews'],
                'unique_Desktop_Jobs_Page_Views': item['totalPageStatistics']['views']['desktopJobsPageViews']['uniquePageViews'],
                'jobs_Page_Views': item['totalPageStatistics']['views']['jobsPageViews']['pageViews'],
                'unique_Jobs_Page_Views': item['totalPageStatistics']['views']['jobsPageViews']['uniquePageViews'],
                'people_Page_Views': item['totalPageStatistics']['views']['peoplePageViews']['pageViews'],
                'unique_People_Page_Views': item['totalPageStatistics']['views']['peoplePageViews']['uniquePageViews'],
                'desktop_People_Page_Views': item['totalPageStatistics']['views']['desktopPeoplePageViews']['pageViews'],
                'unique_Desktop_People_Page_Views': item['totalPageStatistics']['views']['desktopPeoplePageViews']['uniquePageViews'],
                'about_Page_Views': item['totalPageStatistics']['views']['aboutPageViews']['pageViews'],
                'unique_About_Page_Views': item['totalPageStatistics']['views']['aboutPageViews']['uniquePageViews'],
                'desktop_About_Page_Views': item['totalPageStatistics']['views']['desktopAboutPageViews']['pageViews'],
                'unique_Desktop_About_Page_Views': item['totalPageStatistics']['views']['desktopAboutPageViews']['uniquePageViews'],
                'overview_Page_Views': item['totalPageStatistics']['views']['overviewPageViews']['pageViews'],
                'unique_Overview_Page_Views': item['totalPageStatistics']['views']['overviewPageViews']['uniquePageViews'],
                'mobile_People_Page_Views': item['totalPageStatistics']['views']['mobilePeoplePageViews']['pageViews'],
                'unique_Mobile_People_Page_Views': item['totalPageStatistics']['views']['mobilePeoplePageViews']['uniquePageViews'],
                'desktop_Insights_Page_Views': item['totalPageStatistics']['views']['desktopInsightsPageViews']['pageViews'],
                'unique_Desktop_Insights_Page_Views': item['totalPageStatistics']['views']['desktopInsightsPageViews']['uniquePageViews'],
                'desktop_Careers_Page_Views': item['totalPageStatistics']['views']['desktopCareersPageViews']['pageViews'],
                'unique_Desktop_Careers_Page_Views': item['totalPageStatistics']['views']['desktopCareersPageViews']['uniquePageViews'],
                'mobile_Overview_Page_Views': item['totalPageStatistics']['views']['mobileOverviewPageViews']['pageViews'],
                'unique_Mobile_Overview_Page_Views': item['totalPageStatistics']['views']['mobileOverviewPageViews']['uniquePageViews'],
                'life_At_Page_Views': item['totalPageStatistics']['views']['lifeAtPageViews']['pageViews'],
                'unique_Life_At_Page_Views': item['totalPageStatistics']['views']['lifeAtPageViews']['uniquePageViews'],
                'desktop_Overview_Page_Views': item['totalPageStatistics']['views']['desktopOverviewPageViews']['pageViews'],
                'unique_Desktop_Overview_Page_Views': item['totalPageStatistics']['views']['desktopOverviewPageViews']['uniquePageViews'],
                'desktop_Life_At_Page_Views': item['totalPageStatistics']['views']['desktopLifeAtPageViews']['pageViews'],
                'unique_Desktop_Life_At_Page_Views': item['totalPageStatistics']['views']['desktopLifeAtPageViews']['uniquePageViews'],
                'mobile_Careers_Page_Views': item['totalPageStatistics']['views']['mobileCareersPageViews']['pageViews'],
                'unique_Mobile_Careers_Page_Views': item['totalPageStatistics']['views']['mobileCareersPageViews']['uniquePageViews'],
                'all_Page_Views': item['totalPageStatistics']['views']['allPageViews']['pageViews'],
                'unique_All_Page_Views': item['totalPageStatistics']['views']['allPageViews']['uniquePageViews'],
                'mobile_Insights_Page_Views': item['totalPageStatistics']['views']['mobileInsightsPageViews']['pageViews'],
                'unique_Mobile_Insights_Page_Views': item['totalPageStatistics']['views']['mobileInsightsPageViews']['uniquePageViews']
            })

            writer.writerow(row)
            print('DATE --- {}'.format(parse_timestump_to_date(item['timeRange']['start'])))
        print('*' * 200)


def get_share_statistic_from_response(campaign, start_date_timestump, end_date_timestump):
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


def get_follower_statistic_from_response(campaign, start_date_timestump, end_date_timestump):
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


def get_page_statistic_data_from_response(campaign, start_date_timestump, end_date_timestump):
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
    # extract_page_statistics()
    # extract_data(start_date='2017-01-04', end_date='2019-04-01')
    # extract_daily()

    # extract_daily_share_statistic()
    # extract_share_data(start_date='2018-05-01', end_date='2019-05-14')
    # extract_daily_page_statistic()
    extract_page_statistics(start_date='2019-05-11', end_date='2019-05-12')

