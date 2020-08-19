import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool


def data_to_s3(frmt):
    # throws error occured if there was a problem accessing data
    # otherwise downloads and uploads to s3

    source_dataset_url = 'https://www.cryptodatadownload.com/cdd/Binance_'
    

    try:
        response = urlopen(source_dataset_url + frmt)

    except HTTPError as e:
        raise Exception('HTTPError: ', e.code, frmt)

    except URLError as e:
        raise Exception('URLError: ', e.reason, frmt)

    else:
        data_set_name = os.environ['DATA_SET_NAME']
        filename = data_set_name + frmt
        file_location = '/tmp/' + filename

        with open(file_location, 'wb') as f:
            f.write(response.read())
            f.close()

        # variables/resources used to upload to s3
        s3_bucket = os.environ['S3_BUCKET']
        new_s3_key = data_set_name + '/dataset/'
        s3 = boto3.client('s3')

        s3.upload_file(file_location, s3_bucket, new_s3_key + filename)

        print('Uploaded: ' + filename)

        # deletes to preserve limited space in aws lamdba
        os.remove(file_location)

        # dicts to be used to add assets to the dataset revision
        return {'Bucket': s3_bucket, 'Key': new_s3_key + filename}


def source_dataset():

    # list of enpoints to be used to access data included with product
    data_endpoints = [
        'BTCUSDT_d.csv',
        'ETHUSDT_d.csv',
        'LTCUSDT_d.csv',
        'NEOUSDT_d.csv',
        'ADABTC_d.csv',
        'ASTBTC_d.csv',
        'BTGBTC_d.csv',
        'DASHBTC_d.csv',
        'EOSBTC_d.csv',
        'ETHBTC_d.csv',
        'IOTABTC_d.csv',
        'LTCBTC_d.csv',
        'NEOBTC_d.csv',
        'SALTBTC_d.csv',
        'STRATBTC_d.csv',
        'WTCBTC_d.csv',
        'XMRBTC_d.csv',
        'XLMBTC_d.csv',
        'XRPBTC_d.csv',
        'XLMETH_d.csv',
        'XMRETH_d.csv',
        'XRPETH_d.csv',
        'NEOETH_d.csv',
        'LSKETH_d.csv',
        'IOTAETH_d.csv',
        'ETCETH_d.csv',
        'DASHETH_d.csv',
        'BTGETH_d.csv',

        'BTCUSDT_1h.csv',
        'ETHUSDT_1h.csv',
        'LTCUSDT_1h.csv',
        'NEOUSDT_1h.csv',
        'ADABTC_1h.csv',
        'ASTBTC_1h.csv',
        'BTGBTC_1h.csv',
        'DASHBTC_1h.csv',
        'EOSBTC_1h.csv',
        'ETHBTC_1h.csv',
        'IOTABTC_1h.csv',
        'LTCBTC_1h.csv',
        'NEOBTC_1h.csv',
        'SALTBTC_1h.csv',
        'STRATBTC_1h.csv',
        'WTCBTC_1h.csv',
        'XMRBTC_1h.csv',
        'XLMBTC_1h.csv',
        'XRPBTC_1h.csv',
        'XLMETH_1h.csv',
        'XMRETH_1h.csv',
        'XRPETH_1h.csv',
        'NEOETH_1h.csv',
        'LSKETH_1h.csv',
        'IOTAETH_1h.csv',
        'ETCETH_1h.csv',
        'DASHETH_1h.csv',
        'BTGETH_1h.csv'  

    ]

    # multithreading speed up accessing data, making lambda run quicker
    with (Pool(2)) as p:
        asset_list = p.map(data_to_s3, data_endpoints)

    # asset_list is returned to be used in lamdba_handler function
    return asset_list

