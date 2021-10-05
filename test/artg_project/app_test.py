import os
import re
import copy
import json
import requests
from requests import Request, Session
from datetime import datetime
import time
import send_artgentry_tosql_test


proxy_dict = None

def set_proxy_info(username,password):
    http_proxy  = "http://{}:{}@webproxy.gslb.health.nsw.gov.au:8080".format(username,password)
    https_proxy = "https://{}:{}@webproxy.gslb.health.nsw.gov.au:8080".format(username,password)
    #ftp_proxy   = "ftp://10.10.1.10:3128"

    proxy_dict = {
                  "http"  : http_proxy,
                  "https" : https_proxy
                  #"ftp"   : ftp_proxy
                }
    return proxy_dict

def get_json_fromfile(path):
    print('parsing json file "{}"'.format(path))
    json_data = json.load(open(path))
    return json_data

class Dump:
    """plain data dump"""
    def __init__(self,dirname,data, name=None):
        self.dirname = dirname
        self.data = data
        self.name = name
        #self.totext(dirname,data,name)
    def totext(self):
        timestamp = str(datetime.now())
        timestamp = re.sub("[-:.]","",timestamp)
        if self.name is not None:
            filename = "dump_" + self.name
        else:
            filename = "dump"
        path = os.path.join(self.dirname,filename + timestamp + ".txt")
        try:
            file = open(path,"w")
            if type(self.data) is list or type(self.data) is tuple:
                for row in self.data:
                    try:
                        file.write(str(row)+ '\n') #
                    except Exception as e:
                        print('warning! {} with reference to writing row {}.'.format(e,row))
            else:
                file.write(str(self.data))
        except Exception as e:
            print('warning! {} with reference to dumping object.'.format(e))
            pass

    def tocsv(self):
        timestamp = str(datetime.now())
        timestamp = re.sub("[-:.]","",timestamp)
        if self.name is not None:
            filename = "dump_" + self.name
        else:
            filename = "dump"
        path = os.path.join(self.dirname,filename + timestamp + ".csv")
        try:
            file = open(path,"w")
            if type(self.data) is list or type(self.data) is tuple:
                i = 0
                for row in self.data:
                    columnname_list = list(row.keys())
                    data_list = list(row.values())
                    try:
                        if i == 0: #columns and data
                            file.write(','.join(columnname_list) + '\n')
                            file.write(','.join(str(x) for x in data_list) + '\n')
                        if i > 0: #data only
                            file.write(','.join(str(x) for x in data_list) + '\n')
                    except Exception as e:
                        print('warning! {} with reference to writing row {}.'.format(e,row))

                    i += 1
            else:
                file.write(str(self.data))
        except Exception as e:
            print('warning! {} with reference to dumping object.'.format(e))
            pass

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
                print(msg)
        else:
            print('warning!!! no result data return from {}'.format(proc_units.prepped_req[0].url))
            #production send_artgentry_tosql.process(json_data,session_dt_string = session_dt_string)


def get_data_fromurl(url,session):
    url_string = url
    data = None

    try:
        # determine if proxy network to be used
        if proxy_dict is not None:
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
        d = Dump(r"C:\Users\60145210\Documents\ARTG\dumps",jsonddecodeerr,"jsonddecodeerr")
        d.totext()
    except requests.exceptions.Timeout as e:
        # do some retries
        time.sleep(1)
        tries = 1 #initial try
        timeouterr = []
        while tries <= 3:
            try:
                if proxy_dict is not None:
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
                d = Dump(r"C:\Users\60145210\Documents\ARTG\dumps",timeouterr,"timeouterr")
                d.totext()


    except requests.exceptions.TooManyRedirects:
        msg ='Error too many redirects {} when processing page {}'.format(e,url_string)
        print(msg)
        redirecterrors = []
        redirecterrors.append(msg)
        d = Dump(r"C:\Users\60145210\Documents\ARTG\dumps",redirecterrors,"redirecterr")
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
                if proxy_dict is not None:
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
                d = Dump(r"C:\Users\60145210\Documents\ARTG\dumps",fatalerr,"fatalerr")
                d.totext()


    return data
class ARTG_ProcessUnit:
    def __init__(self,session,prepped_req,jc,data_injection_lod):
        self.session = session # requests's session object
        self.prepped_req = prepped_req #preppared request object
        self.jsontabs_cf_path  = jc #jsonpath to tables config instance
        self.data_injection_lod = data_injection_lod
