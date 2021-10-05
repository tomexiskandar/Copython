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
    # test = "Prevention of onset of lactation in the puerperium for clearly defined medical reasons. Therapy should be continued for 14 days to prevent rebound lactation. Parlodel should not be used to suppress established lactation. Treatment of hyperprolactinaemia where surgery and/or radiotherapy are not indicated or have already been used with incomplete resolution. Precautions should be taken to ensure that the hyperprolactinaemia is not due to severe primary hypothyroidism. Where the cause of hyperprolactinaemia is a prolactin-secreting microadenoma or macroadenoma, Parlodel is indicated for conservative treatment; prior to surgery in order to reduce tumour size and to facilitate removal; after surgery if prolactin level is still elevated. Adjunctive therapy in the management of acromegaly when: (1) The patient refuses surgery and/or radiotherapy; (2) Surgery and/or radiotherapy has been unsuccessful or full effects are not expected for some months; (3) A manifestation of the acromegaly needs to be brought under control pending surgery and/or radiotherapy. Idiopathic or post-encephalitic Parkinson's disease. It should be noted that data are not yet sufficient to evaluate the role of Parlodel in treating early Parkinsonism."
    # print(len(test))
    # quit()
    #example of datapath_list   ['Results', 0, 'Manufacturers', 1, 'Address', 'AddressLine1']
    #example of base_path       ['Results', '#', 'Manufacturers', '#']
    #example of return          [{Result/#=0},{Result/#/Manufacturers/#=1}]
    # bp_list =   ['Results', '#', 'Manufacturers', '#']
    # datapath =  ['Results', 0, 'Manufacturers', 1, 'Address', 'AddressLine1']
    # pathids = []
    # for i,item in enumerate(bp_list):
    #     print(i,item)
    #     if item == '#':
    #         value = datapath[i]
    #         phid = []
    #
    #         for j in range(i+1):
    #             print('j',j)
    #
    #             phid.append(bp_list[j])
    #         print("phid",phid)
    #         _pdict = {'/'.join(phid):value}
    #         print('_pdict',_pdict)
    #         pathids.append(_pdict)
    # print(pathids)

    start = time.time()
    errors = []

    ############################
    # define json data source
    ############################
    #json_path = r"C:\Users\60145210\Documents\ARTG\test2pages3nodes.json"
    json_path = r"C:\Users\60145210\Documents\ARTG\page1-1000.json"
    json_data = get_json_fromfile(json_path)

    ############################
    # define jstabs config file
    ############################
    jsontabs_cf_path = r"C:\Users\60145210\Documents\Projects\copython\test\artg_project\_cf_path_totable2.json"

    ############################
    #define if there is any data injection required
    ############################
    data_injection_lod = []
    session_dt_string = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    data_injection_lod.append({"table_name":"results_artg","column_name":"@created_on_utc","value":session_dt_string})
    data_injection_lod.append({"table_name":"results_artg","column_name":"@data_source","value":"apps.tga.gov.au"})


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

            send_artgentry_tosql_test.process(json_data,jsontabs_cf_path,data_injection_lod)


    finish = time.time()
    duration = int(finish - start)
    print('duration: {} seconds'.format(duration))

    # errors = []
    # page = 58520
    # print('start page# {}'.format(page))
