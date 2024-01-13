import requests, logging
import time
from datetime import datetime
from utils.core.helpers.helpers import previous_quarter

def check_for_new_fdic_data(hook, today, last_quarter):
    found_new_data = False

    # check raw.fdic_institutions for a repdte of the last quarter
    last_q_str = last_quarter.strftime("%Y%m%d")
    sql = f"""
        SELECT
            COUNT(*)
        FROM raw.fdic_institutions
        WHERE repdte = '{last_q_str}'
    """
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    if count > 0:
        logging.info("Already have FDIC data for last quarter")
        return found_new_data

    res_json = fdic_request("/financials", offset=0, limit=10, today_dt=today, last_quarter_dt=last_quarter)
    if res_json['meta']['total'] > 0:
        logging.info("Found new FDIC data")
        found_new_data = True
        return found_new_data
    else:
        logging.info("Did not find new FDIC data")
        return found_new_data

def fdic_request(endpoint, today_dt, last_quarter_dt, offset=0, limit=10000, timeout=1):
    url = 'https://banks.data.fdic.gov/api' + endpoint
    params = {
        'limit': limit,
        'offset': offset,
    }

    if endpoint == '/financials':
        # need to add a filter for REPDTE, which is a quarterly date value formatted like YYYYMMDD
        today = today_dt.strftime('%Y%m%d')
        last_quarter = last_quarter_dt.strftime('%Y%m%d')

        ## DEBUG set today to 2023-12-01 ##
        #today = datetime.datetime(2023, 12, 1).strftime('%Y%m%d')
        ###################################

        params['filters'] = f'REPDTE:["{last_quarter}" TO "{today}"]'

    print('params: ', params)

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"Request failed with status {response.status_code}")

    if response.status_code == 429:
        time.sleep(timeout)
        return fdic_request(endpoint, offset, limit, timeout * 2)
    
    return response.json()

def get_fdic_data(endpoint, today_dt=datetime.today(), last_quarter_dt=previous_quarter(datetime.today())):
    limit = 10000
    offset = 0
    data = []
    while True:
        response = fdic_request(endpoint, today_dt=today_dt, last_quarter_dt=last_quarter_dt, offset=offset, limit=limit)
        data.extend(response["data"])
        if len(response["data"]) < limit:
            break
        offset += limit
    return data