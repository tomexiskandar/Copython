from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import urllib.request, json

import send_artgentry_tosql_test
import time
import datetime
import app_test
import sys, argparse
import getpass
import copy
from requests import Session


#lastpage_called 92845


if __name__=="__main__":
    # if len(sys.argv[1:]) >0:
    #     print(sys.argv[1:])
    parser = argparse.ArgumentParser()
    parser.add_argument('--start',help='the pagestart of the artg search')
    parser.add_argument('--end',help='the pageend of the artg search')
    parser.add_argument('--batch_size',help='number of pages for each of the artg search')
    parser.add_argument('--path',help='json file path to process as a test')
    args = parser.parse_args()
    # param initiation values
    startpage = 0
    endpage = 0
    batch_size = 1
    json_path = None
    if args.start:
        startpage = int(args.start)
    if args.end:
        endpage = int(args.end)
    if args.batch_size:
        batch_size = int(args.batch_size)

    #check if this is a json file load
    if args.path:
        json_path = args.path

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



    # jsontab config file
    jsontabs_cf_path = r"C:\Users\60145210\Documents\Projects\copython\test\artg_project\_cf_jsonpath_totable.json"

    # generate data injection
    data_injection_lod = []
    #session_dt_string = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    session_dt_string = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_injection_lod.append({"table_name":"results_lic","column_name":"@created_on_utc","value":session_dt_string})

    # for pu in proc_units:
    #     print(pu.url,pu.jc.tables[0].name,pu.data_injection_lod)
    #json_path = r"C:\Users\60145210\Documents\ARTG\licence239107.json"
    #json_path = r"C:\Users\60145210\Documents\ARTG\licence311008.json"

    # if json_path is used then use it, otherwise use urls list
    if json_path:
        json_data = app_test.get_json_fromfile(json_path)
        data_injection_lod.append({"table_name":"results_lic","column_name":"@data_source","value":json_path})
        send_artgentry_tosql_test.process(json_data,jsontabs_cf_path,data_injection_lod)
    else:
        # generate url
        #urls = get_artg_urls(startpage,endpage,batch_size)
        # generate requests with params (with params meants so we can reuse params for data injection)
        pagenum_pairs = app_test.get_pagenum_pairs(startpage,endpage,batch_size)
        base_url = 'http://apps.tga.gov.au/prod/ARTGSearch/ARTGWebService.svc/json/ARTGValueSearch/?'
        # params = {
        #     'pagestart': tup[0],
        #     'pageend': tup[1],
        #     # more key=value pairs as appeared in your query string
        # }
        params_template = {"pagestart":"","pageend":""}
        prepped_reqs_wparams = app_test.gen_prepped_requests_withparams(base_url,params_template,pagenum_pairs)
        # for req in prepped_reqs_wparams:
        #     print(req[1]["pagestart"],req[1]["pageend"])
        # quit()
        # create processes list
        proc_units = []
        session = Session()
        #print(urls)
        for preq in prepped_reqs_wparams:
            di_lod = copy.deepcopy(data_injection_lod)
            di_lod.append({"table_name":"results_lic","column_name":"@data_source","value":base_url})
            params_list = [k + '={}' for k in params_template]
            # print('&'.join(params_list))
            # quit()
            di_lod.append({"table_name":"results_lic","column_name":"@params_template","value":'&'.join(params_list)})
            param_value = json.dumps(preq[1])
            di_lod.append({"table_name":"results_lic","column_name":"@params","value":param_value})

            pu = app_test.ARTG_ProcessUnit(session,preq,jsontabs_cf_path,di_lod)
            proc_units.append(pu)

        pool = ThreadPool(10)
        pool.map(app_test.load_json, proc_units)
        pool.close()
        pool.join()

    finish = time.time()
    duration = int(finish - start)
    print('duration: {} seconds'.format(duration))
