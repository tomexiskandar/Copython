from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import urllib.request, json
import requests
import send_artgentry_tosql_test
import time
import datetime
import app_test

def get_json_fromfile(path):
    print('parsing json file "{}"'.format(path))
    json_data = json.load(open(path))
    return json_data

def load_json_tosql(data):
    send_artgentry_tosql_test.process(data)


if __name__=="__main__":
    start = time.time()
    errors = []
    path = r"C:\Users\60145210\Documents\ARTG\test2pages3nodes.json"
    session_dt = datetime.datetime.now(datetime.timezone.utc)
    session_dt_string = session_dt.isoformat(timespec='microseconds')
    json_data = get_json_fromfile(path)
    if json_data is None:
        pass
    else:
        res = len(json_data['Results'])
        if res:
            # try:
            #     print('loading json data...')
            #     send_artgentry_tosql.process(json_data,session_dt_string = session_dt_string)
            # except Exception as e:
            #     msg = 'error {} when loading json {}'.format(e,path)
            #     errors.append(msg)
            send_artgentry_tosql_test.process(json_data,session_dt_string = session_dt_string)


    finish = time.time()
    duration = int(finish - start)
    print('duration: {} seconds'.format(duration))

    # errors = []
    # page = 58520
    # print('start page# {}'.format(page))
