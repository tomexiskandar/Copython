from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import urllib.request, json
import requests
import send_artgentry_tosql_test
import time
import datetime
import app_test
import sys, argparse
import getpass
import copy
from requests import Request, Session
import json



def get_json_fromfile(path):
    print('parsing json file "{}"'.format(path))
    json_data = json.load(open(path))
    return json_data

def gen_prepped_requests_withparams(base_url,params_template,pagenum_pairs):
    reqs = []
    for tup in pagenum_pairs:
        params = copy.deepcopy(params_template)
        params["pagestart"] = tup[0]
        params["pageend"]   = tup[1]

        req = Request('GET', base_url, params=params).prepare()
        req_tup = (req,params)
        reqs.append(req_tup)
    return reqs


def get_artg_urls(start,end,batch_size):
    batch_size = batch_size if (end-start+1) >= batch_size else (end-start+1)
    batches = int((end-start+1)/batch_size)
    pagenum_lot = []
    for i in range(1,batches + 1):
        tup = ((i*batch_size)-batch_size + start, i*batch_size + start -1)
        pagenum_lot.append(tup)

    # add any remaining page
    if (end-start+1) % batch_size:
        tup = ((batches*batch_size) + start , (batches*batch_size) + start + ((end-start) % batch_size))
        pagenum_lot.append(tup)

    urls = []
    for tup in pagenum_lot:
        _dict = {}
        _dict["url"] = "http://apps.tga.gov.au/prod/ARTGSearch/ARTGWebService.svc/json/ARTGValueSearch/?pagestart={}&pageend={}".format(tup[0],tup[1])
        _dict["pagestart"] = tup[0]
        _dict["pageend"]   = tup[1]
        urls.append(_dict)
    return urls

def get_pagenum_pairs(start,end,batch_size):
    batch_size = batch_size if (end-start+1) >= batch_size else (end-start+1)
    batches = int((end-start+1)/batch_size)
    pagenum_pairs = []
    for i in range(1,batches + 1):
        tup = ((i*batch_size)-batch_size + start, i*batch_size + start -1)
        pagenum_pairs.append(tup)

    # add any remaining page
    if (end-start+1) % batch_size:
        tup = ((batches*batch_size) + start , (batches*batch_size) + start + ((end-start) % batch_size))
        pagenum_pairs.append(tup)
    return pagenum_pairs


def load_json(proc_units):
    #proc unit attributes
    # self.session = session # requests's session object
    # self.prepped_req = prepped_req #preppared request object
    # self.jsontabs_cf_path  = jsontabs_cf_path #jsonpath to tables config instance
    # self.data_injection_lod = data_injection_lod




    json_data = get_data_fromurl(proc_units.prepped_req[0].url,proc_units.session)
    #print('--json_data-->',json_data)
    if json_data is None:
        print('warning!!! no json data return from {}'.format(proc_units.prepped_req[0].url))

    else:
        res = len(json_data['Results'])
        if res:
            try:
                print('loading json from {}'.format(proc_units.prepped_req[0].url))
                #send_artgentry_tosql.process(json_data,session_dt_string = session_dt_string)

                send_artgentry_tosql_test.process(json_data,proc_units.jsontabs_cf_path,proc_units.data_injection_lod)
            except Exception as e:
                msg = 'error {} when loading json from {}'.format(e,proc_units.prepped_req[0].url)
                errors.append(msg)
        else:
            print('warning!!! no result data return from {}'.format(proc_units.prepped_req[0].url))
            #production send_artgentry_tosql.process(json_data,session_dt_string = session_dt_string)


def get_data_fromurl(url,session):
    url_string = url
    data = None

    try:
        # determine if proxy network to be used
        if app_test.proxy_dict is not None:
            resp = session.get(url = url,timeout=30,proxies=proxy_dict) #,proxies=proxy_dict
        else:
            resp = session.get(url = url,timeout=30)
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
                    resp = session.get(url = url,timeout=30,proxies=proxy_dict) #,proxies=proxy_dict
                else:
                    resp = session.get(url = url,timeout=30)
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
                    resp = session.get(url = url,timeout=30,proxies=proxy_dict) #,proxies=proxy_dict
                else:
                    resp = session.get(url = url,timeout=30)
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
class ARTG_ProcessUnit:
    def __init__(self,session,prepped_req,jc,data_injection_lod):
        self.session = session # requests's session object
        self.prepped_req = prepped_req #preppared request object
        self.jsontabs_cf_path  = jsontabs_cf_path #jsonpath to tables config instance
        self.data_injection_lod = data_injection_lod


if __name__=="__main__":
    # if len(sys.argv[1:]) >0:
    #     print(sys.argv[1:])
    parser = argparse.ArgumentParser()
    parser.add_argument('--start',help='the pagestart of the artg search')
    parser.add_argument('--end',help='the pageend of the artg search')
    parser.add_argument('--batch_size',help='number of pages for each of the artg search')
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
    batch_size = 1
    if args.batch_size:
        batch_size = int(args.batch_size)


    # generate url
    #urls = get_artg_urls(startpage,endpage,batch_size)
    # generate requests with params (with params meants so we can reuse params for data injection)
    session = Session()

    pagenum_pairs = get_pagenum_pairs(startpage,endpage,batch_size)
    base_url = 'http://apps.tga.gov.au/prod/ARTGSearch/ARTGWebService.svc/json/ARTGValueSearch/?'
    # params = {
    #     'pagestart': tup[0],
    #     'pageend': tup[1],
    #     # more key=value pairs as appeared in your query string
    # }
    params_template = {"pagestart":"","pageend":""}
    prepped_reqs_wparams = gen_prepped_requests_withparams(base_url,params_template,pagenum_pairs)

    # for req in prepped_reqs_wparams:
    #     print(req[1]["pagestart"],req[1]["pageend"])
    # quit()


    # jsontab config file
    jsontabs_cf_path = r"C:\Users\60145210\Documents\Projects\copython\test\artg_project\_cf_jsonpath_totable.json"
    # generate data injection
    data_injection_lod = []
    session_dt_string = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    data_injection_lod.append({"table_name":"results_lic","column_name":"@created_on_utc","value":session_dt_string})
    data_injection_lod.append({"table_name":"results_lic","column_name":"@data_source","value":base_url})
    # params_template = {"pagestart":"","pageend":""}
    params_list = [k + '={}' for k in params_template]
    # print('&'.join(params_list))
    # quit()
    data_injection_lod.append({"table_name":"results_lic","column_name":"@params_template","value":'&'.join(params_list)})


    # create processes list
    proc_units = []
    #print(urls)
    for preq in prepped_reqs_wparams:
        di_lod = copy.deepcopy(data_injection_lod)
        #di_lod.append({"table_name":"results_lic","column_name":"param_pagestart","value":preq[1]["pagestart"]})
        #di_lod.append({"table_name":"results_lic","column_name":"param_pageend","value":preq[1]["pageend"]})
        param_value = json.dumps(preq[1])
        di_lod.append({"table_name":"results_lic","column_name":"@params","value":param_value})

        pu = ARTG_ProcessUnit(session,preq,jsontabs_cf_path,di_lod)
        proc_units.append(pu)
        #print(url)
    # for pu in proc_units:
    #     print(pu.url,pu.jc.tables[0].name,pu.data_injection_lod)
    #path = r"C:\Users\60145210\Documents\ARTG\licence239107.json"
    #path = r"C:\Users\60145210\Documents\ARTG\licence311008.json"
    path = r"C:\Users\60145210\Documents\ARTG\page1.json"
    json_data = get_json_fromfile(path)
    send_artgentry_tosql_test.process(json_data,jsontabs_cf_path,data_injection_lod)

    # pool = ThreadPool(10)
    # pool.map(load_json, proc_units)
    # pool.close()
    # pool.join()
    finish = time.time()
    duration = int(finish - start)
    print('duration: {} seconds'.format(duration))
