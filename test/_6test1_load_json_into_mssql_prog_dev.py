from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import urllib.request, json
import requests
import send_artgentry_tosql
import time
import datetime
import app_test
import sys, argparse
import getpass

def get_artg_urls(start,end):

    urls = []
    for i in range(start,end + 1):
        url = "http://apps.tga.gov.au/prod/ARTGSearch/ARTGWebService.svc/json/ARTGValueSearch/?entrytype=device&pagestart={}&pageend={}".format(i,i)
        urls.append(url)
    return urls

def load_json(url):
    # get the session timestamp for this data
    session_dt = datetime.datetime.now(datetime.timezone.utc)
    session_dt_string = session_dt.isoformat(timespec='microseconds')
    json_data = get_data_fromurl(url)
    if json_data is None:
        print('warning!!! no json data return from {}'.format(url))

    else:
        res = len(json_data['Results'])
        if res:
            try:
                print('loading json from {}'.format(url))
                send_artgentry_tosql.process(json_data,session_dt_string = session_dt_string)
            except Exception as e:
                msg = 'error {} when loading json from {}'.format(e,url)
                errors.append(msg)
        else:
            print('warning!!! no result data return from {}'.format(url))
            #production send_artgentry_tosql.process(json_data,session_dt_string = session_dt_string)


def get_data_fromurl(url):
    url_string = url
    data = None

    # http_proxy  = "http://60145210:XXXXX@webproxy.gslb.health.nsw.gov.au:8080"
    # https_proxy = "https://60145210:XXXX@webproxy.gslb.health.nsw.gov.au:8080"
    # #ftp_proxy   = "ftp://10.10.1.10:3128"
    #
    # proxy_dict = {
    #               "http"  : http_proxy,
    #               "https" : https_proxy
    #               #"ftp"   : ftp_proxy
    #             }
    try:
        # determine if proxy network to be used
        if app_test.proxy_dict is not None:
            resp = requests.get(url = url,timeout=30,proxies=proxy_dict) #,proxies=proxy_dict
        else:
            resp = requests.get(url = url,timeout=30)
        data = resp.json()

    except json.decoder.JSONDecodeError as e:
        msg ='Error json.decoder {} when processing page {}'.format(e,url_string)
        print(msg)
        jsonddecodeerr = []
        jsonddecodeerr.append(msg)
        jsonddecodeerr.append('one test revealed that incorrect password in proxy would return this error.')
        d = app_test.Dump(r"C:\Users\60145210\Documents\ARTG\dumps",jsonddecodeerr,"jsonddecodeerr")
        d.totext()
    except requests.exceptions.Timeout as e:
        # do some retries
        time.sleep(1)
        tries = 1 #initial try
        timeouterr = []
        while tries <= 3:
            try:
                if app_test.proxy_dict is not None:
                    resp = requests.get(url = url,timeout=30,proxies=proxy_dict) #,proxies=proxy_dict
                else:
                    resp = requests.get(url = url,timeout=30)
                data = resp.json()
            except requests.exceptions.Timeout as e:
                time.sleep(1)
                tries += 1
                msg ='Error timout {} when processing page {}'.format(e,url_string)
                print(msg)
                timeouterr.append(msg)
                d = app_test.Dump(r"C:\Users\60145210\Documents\ARTG\dumps",timeouterr,"timeouterr")
                d.totext()


    except requests.exceptions.TooManyRedirects:
        msg ='Error too many redirects {} when processing page {}'.format(e,url_string)
        print(msg)
        redirecterrors = []
        redirecterrors.append(msg)
        d = app_test.Dump(r"C:\Users\60145210\Documents\ARTG\dumps",redirecterrors,"redirecterr")
        d.totext()
    except requests.exceptions.RequestException as e:
        msg = 'Fatal error {} when processing page {}'.format(e,url_string)
        print(msg)
        fatalerr = []
        fatalerr.append(msg)
        # do some retries
        time.sleep(1)
        tries = 1 #initial try
        while tries <= 3:
            try:
                if app_test.proxy_dict is not None:
                    resp = requests.get(url = url,timeout=30,proxies=proxy_dict) #,proxies=proxy_dict
                else:
                    resp = requests.get(url = url,timeout=30)
                data = resp.json()
                return data
            except requests.exceptions.RequestException as e:
                time.sleep(2)
                tries += 1
                msg ='Fatal error (attempt:{}) {} when processing page {}'.format(tries,e,url_string)
                print(msg)
                fatalerr.append(msg)
                d = app_test.Dump(r"C:\Users\60145210\Documents\ARTG\dumps",fatalerr,"fatalerr")
                d.totext()


    return data


if __name__=="__main__":
    # if len(sys.argv[1:]) >0:
    #     print(sys.argv[1:])
    parser = argparse.ArgumentParser()
    parser.add_argument('--start',help='the pagestart of the artg search')
    parser.add_argument('--end',help='the pageend of the artg search')
    args = parser.parse_args()
    # print(args)
    # print(args.start)
    # print(args.end)
    username = getpass.getuser()
    try:
        print('no password required if you have direct internet')
        password = getpass.getpass('password for {}: '.format(username))
    except Exeception as e:
        print('Error ',e)
    finally:
        if password !='':
            proxy_dict = app_test.set_proxy_info(username,password)
            app_test.proxy_dict = proxy_dict
            #print(app_test.proxy_dict)
        # else:
        #     print('password not given, direct internet (non proxy) is being considered...')
        #     print(app_test.proxy_dict)



    start = time.time()
    errors = []
    startpage = int(args.start)
    endpage = int(args.end)
    urls = get_artg_urls(startpage,endpage) #failed until page# 58581
    # print(urls)
    # for url in urls:
    #     print(url)
    # quit()

    pool = ThreadPool(20)
    pool.map(load_json, urls)
    pool.close()
    pool.join()
    finish = time.time()
    duration = int(finish - start)
    print('duration: {} seconds'.format(duration))

    # errors = []
    # page = 58520
    # print('start page# {}'.format(page))
