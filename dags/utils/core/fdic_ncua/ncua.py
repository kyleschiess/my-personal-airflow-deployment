import zipfile
import csv
import logging
import requests, io

from utils.core.helpers.helpers import convert_to_snake_case

def check_for_new_ncua_data(hook, last_quarter, lq_year, lq_month):
    found_new_data = False

    # check raw.ncua_fs220 for a cycle_date of the last quarter
    last_q_str = last_quarter.strftime("%Y-%m-%d")
    sql = f"""
        SELECT
            COUNT(*)
        FROM raw.ncua_fs220
        WHERE cycle_date::DATE = '{last_q_str}'
    """
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    if count > 0:
        logging.info("Already have NCUA data for last quarter")
        return found_new_data
    
    url = f"https://ncua.gov/files/publications/analysis/call-report-data-{lq_year}-{lq_month}.zip"
    response = requests.get(url)
    
    if response.status_code == 200:
        logging.info("Found new NCUA data")
        found_new_data = True
        return found_new_data
    else:
        logging.info("Did not find new NCUA data")
        return found_new_data


def ncua_call_report_to_s3(hook, year, quarter, timeout=1):
    zip_found = False
    url = f"https://ncua.gov/files/publications/analysis/call-report-data-{year}-{quarter}.zip"

    # test if url exists
    response = requests.get(url)
    if response.status_code != 200:
        logging.info(f"Request failed with status {response.status_code}. Check that the url {url} exists.")
        return zip_found

    # read response of url into s3 bucket
    with requests.get(url, stream=True) as r:
        c = r.content
        file_obj = io.BytesIO(c)
        try:
            hook.load_file_obj(file_obj=file_obj, key=f'ncua-call-report-data/{year}-{quarter}.zip', bucket_name='alpharank-de-eval')
        except ValueError as e:
            if "already exists" in e.args[0]:
                logging.info(f"File ncua-call-report-data/{year}-{quarter}.zip already exists in bucket alpharank-de-eval")
            else:
                raise e
            
        zip_found = True
        return zip_found
            

def read_ncua_txt_file_to_array_of_dicts(zfile, encoding='utf-8'):
    data = []
    reader = csv.reader(io.TextIOWrapper(zfile, encoding), delimiter='|')
    headers = next(reader)

    # headers will have a length of 1, with the first item being the entire header string
    # remove double quotes and single quotes from the header string
    headers = headers[0].replace('"', '').replace("'", '').split(',')

    # Special case for cu_number column that's spelled "CU_Number" instead of "CU_NUMBER".
    # Saw this is FS220N.txt
    headers = [h.replace('CU_Number', 'CU_NUMBER') for h in headers]

    # convert headers to snake case
    headers = [convert_to_snake_case(h) for h in headers]

    for row in reader:
        row_reader = csv.reader(io.StringIO(row[0]))
        row = next(row_reader)

        d = {k: v for k, v in zip(headers, row)}
        data.append(d)

    return data

def get_ncua_call_report_file(hook, year, quarter, file_name, encoding='utf-8', retries=1):
    try:
        # get object from bucket
        obj = hook.get_conn().get_object(Bucket='alpharank-de-eval', Key=f'ncua-call-report-data/{year}-{quarter}.zip')

        # this file is a zip that contains a folder with the same name as the zip
        # within the folder are comma-delimited txt files
        # read each of these txt files into arrays of dicts
        with zipfile.ZipFile(io.BytesIO(obj['Body'].read())) as z:
            z.extractall(f'{year}-{quarter}')
            
            # get the txt file from the extracted folder
            with z.open(f'{file_name}') as zfile:
                data = read_ncua_txt_file_to_array_of_dicts(zfile, encoding)
                            
        return data
    
    except UnicodeDecodeError as e:
        print(e)
        if retries > 0:
            print('trying ISO-8859-1')
            return get_ncua_call_report_file(hook, year, quarter, file_name, encoding='ISO-8859-1', retries=retries-1)
        else:
            raise e