import zipfile
import csv
import logging
import requests, io

from utils.core.helpers.helpers import convert_to_snake_case

def ncua_call_report_to_s3(hook, year, quarter, timeout=1):
    url = f"https://ncua.gov/files/publications/analysis/call-report-data-{year}-{quarter}.zip"

    # test if url exists
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Request failed with status {response.status_code}. Check that the url {url} exists.")

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
            

def read_ncua_txt_file_to_array_of_dicts(zfile):
    data = []
    reader = csv.reader(io.TextIOWrapper(zfile, 'utf-8'), delimiter='|')
    headers = next(reader)

    # headers will have a length of 1, with the first item being the entire header string
    # remove double quotes and single quotes from the header string
    headers = headers[0].replace('"', '').replace("'", '').split(',')

    # convert headers to snake case
    headers = [convert_to_snake_case(h) for h in headers]

    for row in reader:
        # row will look like this ['"Okay","Well, actually","3"']
        row_reader = csv.reader(io.StringIO(row[0]))
        row = next(row_reader)

        d = {k: v for k, v in zip(headers, row)}
        data.append(d)

    return data


def get_ncua_call_report_file(hook, year, quarter, file_name):
    # get object from bucket
    obj = hook.get_conn().get_object(Bucket='alpharank-de-eval', Key=f'ncua-call-report-data/{year}-{quarter}.zip')

    # this file is a zip that contains a folder with the same name as the zip
    # within the folder are comma-delimited txt files
    # read each of these txt files into arrays of dicts
    with zipfile.ZipFile(io.BytesIO(obj['Body'].read())) as z:
        z.extractall(f'{year}-{quarter}')
        
        # get the txt file from the extracted folder
        with z.open(f'{file_name}') as zfile:
            data = read_ncua_txt_file_to_array_of_dicts(zfile)
            
    return data