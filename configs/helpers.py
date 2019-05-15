import json
import re
import time
import csv
import snowflake.connector as connector


DAYLOAD = 1

CSV_COLUMNS1 = ['DATE', 'organization_id', 'shareCount', 'uniqueImpressionsCount', 'clickCount', 'engagement',
               'likeCount', 'impressionCount', 'commentCount', 'paidFollowerGain', 'organicFollowerGain'
               ]


CSV_COLUMNS2 = ['DATE',  'mobile_Custom_Button_Click_Counts',
                'careers_Page_Banner_Promo_Clicks', 'careers_Page_Employees_Clicks', 'careers_Page_Promo_Links_Clicks',
                'careers_Page_Jobs_Clicks', 'mobile_Careers_Page_Employees_Clicks',
                'mobile_Careers_Page_Promo_Links_Clicks', 'mobile_Careers_Page_Jobs_Clicks',
                'desktop_Custom_Button_Click_Counts',
                'mobile_Jobs_Page_Views', 'unique_Mobile_Jobs_Page_Views',
                'careers_Page_Views', 'unique_Careers_Page_Views',
                'mobile_Life_At_Page_Views', 'unique_Mobile_Life_At_Page_Views',
                'insights_Page_Views', 'unique_Insights_Page_Views',
                'all_Desktop_Page_Views', 'unique_All_Desktop_Page_Views',
                'mobile_About_Page_Views', 'unique_Mobile_About_Page_Views',
                'all_Mobile_Page_Views', 'unique_All_Mobile_Page_Views',
                'desktop_Jobs_Page_Views', 'unique_Desktop_Jobs_Page_Views',
                'jobs_Page_Views', 'unique_Jobs_Page_Views',
                'people_Page_Views', 'unique_People_Page_Views',
                'desktop_People_Page_Views', 'unique_Desktop_People_Page_Views',
                'about_Page_Views', 'unique_About_Page_Views',
                'desktop_About_Page_Views', 'unique_Desktop_About_Page_Views',
                'overview_Page_Views', 'unique_Overview_Page_Views',
                'mobile_People_Page_Views', 'unique_Mobile_People_Page_Views',
                'desktop_Insights_Page_Views', 'unique_Desktop_Insights_Page_Views',
                'desktop_Careers_Page_Views', 'unique_Desktop_Careers_Page_Views',
                'mobile_Overview_Page_Views', 'unique_Mobile_Overview_Page_Views',
                'life_At_Page_Views', 'unique_Life_At_Page_Views',
                'desktop_Overview_Page_Views', 'unique_Desktop_Overview_Page_Views',
                'desktop_Life_At_Page_Views', 'unique_Desktop_Life_At_Page_Views',
                'mobile_Careers_Page_Views', 'unique_Mobile_Careers_Page_Views',
                'all_Page_Views', 'unique_All_Page_Views',
                'mobile_Insights_Page_Views', 'unique_Mobile_Insights_Page_Views'
                ]

RAW_DB_COLUMNS1 = \
    """
    DATE date,
    organization_id string, 
    shareCount integer, 
    uniqueImpressions integer, 
    clicks integer, 
    engagement float,
    likes integer, 
    impressions integer, 
    comments integer,
    paidFollowerGain integer,
    organicFollowerGain integer
    """

RAW_DB_COLUMNS2 = \
    """
    DATE date, 
    mobile_Custom_Button_Click_Counts integer,
    careers_Page_Banner_Promo_Clicks integer,
    careers_Page_Employees_Clicks integer,
    careers_Page_Promo_Links_Clicks integer,
    careers_Page_Jobs_Clicks integer,
    mobile_Careers_Page_Employees_Clicks integer,
    mobile_Careers_Page_Promo_Links_Clicks integer,
    mobile_Careers_Page_Jobs_Clicks integer,
    desktop_Custom_Button_Click_Counts integer,
    mobile_Jobs_Page_Views integer,
    unique_Mobile_Jobs_Page_Views integer,
    careers_Page_Views integer,
    unique_Careers_Page_Views integer,
    mobile_Life_At_Page_Views integer,
    unique_Mobile_Life_At_Page_Views integer,
    insights_Page_Views integer,
    unique_Insights_Page_Views integer,
    all_Desktop_Page_Views integer,
    unique_All_Desktop_Page_Views integer,
    mobile_About_Page_Views integer, 
    unique_Mobile_About_Page_Views integer,
    all_Mobile_Page_Views integer,
    unique_All_Mobile_Page_Views integer,
    desktop_Jobs_Page_Views integer,
    unique_Desktop_Jobs_Page_Views integer,
    jobs_Page_Views integer,
    unique_Jobs_Page_Views integer,
    people_Page_Views integer,
    unique_People_Page_Views integer,
    desktop_People_Page_Views integer,
    unique_Desktop_People_Page_Views integer,
    about_Page_Views integer,
    unique_About_Page_Views integer,
    desktop_About_Page_Views integer, 
    unique_Desktop_About_Page_Views integer,
    overview_Page_Views integer, 
    unique_Overview_Page_Views integer,
    mobile_People_Page_Views integer,
    unique_Mobile_People_Page_Views integer,
    desktop_Insights_Page_Views integer,
    unique_Desktop_Insights_Page_Views integer,
    desktop_Careers_Page_Views integer,
    unique_Desktop_Careers_Page_Views integer,
    mobile_Overview_Page_Views integer,
    unique_Mobile_Overview_Page_Views integer,
    life_At_Page_Views integer, 
    unique_Life_At_Page_Views integer,
    desktop_Overview_Page_Views integer,
    unique_Desktop_Overview_Page_Views integer,
    desktop_Life_At_Page_Views integer,
    unique_Desktop_Life_At_Page_Views integer,
    mobile_Careers_Page_Views integer,
    unique_Mobile_Careers_Page_Views integer,
    all_Page_Views integer,
    unique_All_Page_Views integer,
    mobile_Insights_Page_Views integer,
    unique_Mobile_Insights_Page_Views integer    
    """

QUERY_STATS_DB_COLUMNS = \
    """SOURCE string, 
    DATE date, 
    TARGET_URL string,
    TOTAL_PAGE_AVG_CLICK_POSITION integer,
    TOTAL_PAGE_AVG_IMPRESSION_POSITION integer,
    TOTAL_PAGE_CLICS integer,
    TOTAL_PAGE_IMPRESSIONS integer,
    QUERY_AVG_CLICK_POSITION integer,
    QUERY_AVG_IMPRESSION_POSITION integer,
    QUERY_CLICKS integer,
    QUERY_IMPRESSIONS integer, 
    QUERY string
    """

V16_DB_COLUMNS = \
    """
    DATE date, 
    PROVIDER string, 
    MEDIUM string,
    CHANNELGROUPING string, 
    DEVICECATEGORY string,
    KEYWORD string,
    FULLURL string,
    ADCLICKS integer,
    IMPRESSIONS integer,
    ISDATAGOLDEN varchar,
    LANDINGPAGEPATH string,
    SOURCE string,
    CAMPAIGN string,
    ADGROUP string,
    SUBCHANNEL string,
    PROFILEID integer,
    CAMPAIGNSTATUS string,
    ADGROUPSTATUS string,
    ADSTATUS string,
    ADCONTENT string,
    PROVIDERCAMPAIGN string,
    EMAILCAMPAIGN string,
    BOUNCES integer,
    TRANSACTIONS integer,
    SESSIONS integer,
    SESSIONDURATION integer,
    USERS integer,
    NEWUSERS integer,
    TRANSACTIONREVENUE integer,
    PAGEVIEWS integer,
    ADCOST NUMBER(38,4),
    IMPRESSIONSHARE varchar,
    SAMPLINGLEVEL string,
    EMAILCLICKS integer,
    EMAILOPENS integer,
    EMAILUNIQUESENDS integer,
    EMAILSENDS integer,
    EMAILHARDBOUNCES integer,
    EMAILSOFTBOUNCES integer,
    EMAILOTHERBOUNCES integer,
    EMAILUNSUBSCRIBES integer,
    VIDEOVIEWS integer,

    goal1completions integer,
    goal2completions integer,
    goal3completions integer,
    goal4completions integer,
    goal5completions integer,
    goal6completions integer,
    goal7completions integer,
    goal8completions integer,
    goal9completions integer,
    goal10completions integer,
    goal11completions integer,
    goal12completions integer,
    goal13completions integer,
    goal14completions integer,
    goal15completions integer,
    goal16completions integer,
    goal17completions integer,
    goal18completions integer,
    goal19completions integer,
    goal20completions integer
    """


# def normalize_backfill_start_end_time(start_date, end_date):
#     end_time = (end_date + timedelta(days=1) - timedelta(seconds=1)).strftime(DATETIME_FORMAT)
#     start_time = start_date.strftime(DATETIME_FORMAT)
#     return start_time, end_time


def establish_db_conn(user, password, account, db, warehouse):
    conn = connector.connect(
        user=user,
        password=password,
        account=account
    )
    conn.cursor().execute('USE DATABASE {}'.format(db))
    conn.cursor().execute('USE WAREHOUSE {}'.format(warehouse))
    return conn


def get_client_config(conf_path, client_name=None):
    if client_name is None:
        with open(conf_path, 'r') as f:
            conf = json.load(f)
    else:
        with open(conf_path, 'r') as f:
            conf = json.load(f).get(client_name)
    assert conf is not None
    return conf


def perform_db_routines(sql, client_config, db_config):

    conn = establish_db_conn(db_config['user'],
                             db_config['pwd'],
                             db_config['account'],
                             client_config['db'],
                             client_config['warehouse'])
    conn.autocommit(False)
    curr = conn.cursor()
    queries_list = sql.split(';')
    try:
        curr.execute('BEGIN')
        for q in queries_list:
            curr.execute(q)
        curr.execute('COMMIT')
    finally:
        curr.close()
        conn.close()


def get_data_by_chunks(items_list, n):
    for i in range(0, len(items_list), n):
        yield items_list[i:i + n]


def parse_date(string_date):
    a = re.search(r'\d+', string_date)
    timestamp = a.group(0)
    return time.strftime("%Y-%m-%d", time.gmtime(int(timestamp) / 1000.0))


def prepare_header_for_clear_csv(file, headers):
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()
    return writer
