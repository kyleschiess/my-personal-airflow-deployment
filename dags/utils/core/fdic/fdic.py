import requests
import time
from datetime import datetime
from utils.core.helpers.helpers import previous_quarter

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