import gzip
import json

def read_folder_to_data(hook, bucket_name, prefix):
    keys = [
        x['Key'] for x in hook.get_conn().list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']
    ]

    data = []
    for key in keys:
        data.append(read_file_to_data(hook, bucket_name, key))

    return data

def read_file_to_data(hook, bucket_name, key):
    # get object from bucket
    obj = hook.get_conn().get_object(Bucket=bucket_name, Key=key)

    if '.json.gz' in key:
        # read json file
        with gzip.open(obj['Body'], 'rb') as f:
            data = f.read()
            j = json.loads(data.decode('utf-8'))
                     
    return j

