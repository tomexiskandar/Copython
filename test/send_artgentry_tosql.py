use_package = False
if use_package is False:
    import sys
    sys.path.insert(0,r"C:\Users\60145210\Documents\Projects\copython")

import json
import copython
from copython import copyconf
import app_test

def traverse(dict_or_list, path=[]):
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    else:
        iterator = enumerate(dict_or_list)
    for k, v in iterator:
        yield path + [k], v,
        if isinstance(v, (dict, list)):
            for k, v in traverse(v, path + [k]):
                yield k, v

def get_tables(paths_dict,path):
    #print(' ->node_list {}'.format(node_list))
    tables = []

    for k,v in paths_dict.items():

        for row in v:
            matches = 0
            nodes = list(filter(None,row.split('/')))
            if len(path)==len(nodes):
                for i,node in enumerate(nodes):
                    #print(i,node)
                    if node == '#':
                        if isinstance(path[i],int):
                            matches += 1
                    else:
                        if path[i] == node:
                            matches += 1
                if len(path) == matches:
                    tables.append(k)
            else:
                continue
    return tables

#example value of data_injection_lod [{"table_name":"artg_entry","column_name":"created_on","value":timestamp}]
def get_tables_with_record(data,paths_dict,data_injection_lod = None):
    tables_dict = {}
    record_dict = {}
    for k in paths_dict:
        tables_dict[k] = [] #initiate an empy list so can append later on
        record_dict[k] = {} #initiate an empty dict so can add later on
    for path, val in traverse(data):
        # map each path to the correct table
        res = get_tables(paths_dict,path)
        if len(res) > 0 is not None:
            #print(res)
            for tab in res:
                #print(record_dict)
                record_dict[tab][path[-1]] = val

    for tab,record in record_dict.items():
        # do data injection
        if data_injection_lod is not None:
            for _di_dict in data_injection_lod:
                if _di_dict["table_name"] == tab:
                    record[_di_dict['column_name']] = _di_dict['value']



        tables_dict[tab].append(record)
        # print('debugging....')
        # if tab == 'artg_entry':
        #     print(' ',record)


    return tables_dict

def process(data,session_dt_string = None):
    # print(data)
    # print(session_dt_string)
    # drop target table (or comment the two lines below to append data into an existing table)
    #conn_str = "DRIVER={ODBC Driver 13 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=ARTG;UID=tester1;PWD=password;"
    #copython.drop_table(conn_str,"dbo","artg_entry")

    #----------------------------------------
    # start writing a simple programmable copy
    #----------------------------------------
    # define a dict that holds paths from different group in json (think this as schema)
    paths_dict = {}
    colmap_dict = {}

    # define if there is any data injection required
    data_injection_lod = []
    data_injection_lod.append({"table_name":"artg_entry","column_name":"created_on","value":session_dt_string})
    data_injection_lod.append({"table_name":"artg_entry","column_name":"data_source","value":"apps.tga.gov.au"})

    ##### prepare artg_entry table #############
    # create artg_entry paths
    artg_entry = ['/Results/#/ApprovalArea'\
                  ,'/Results/#/EntryType'\
                  ,'/Results/#/LicenceClass'\
                  ,'/Results/#/LicenceId'\
                  ,'/Results/#/Name'\
                  ,'/Results/#/ProductCategory'\
                  ,'/Results/#/ProductInformation/DocumentLink'\
                  ,'/Results/#/StartDate'\
                  ,'/Results/#/Status'\
                  ]
    # add the path dict
    paths_dict['artg_entry'] = artg_entry
    #colmap for artg_entry
    colmap_artg_entry_dict = {'ApprovalArea':'ApprovalArea'
                         ,'EntryType':'EntryType'
                         ,'LicenceClass':'LicenceClass'
                         ,'LicenceId':'LicenceId'
                         ,'Name':'Name'
                         ,'ProductCategory':'ProductCategory'
                         ,'DocumentLink':'DocumentLink'
                         ,'StartDate':'StartDate'
                         ,'Status:':'Status'
                         ,'created_on':'created_on' #injected
                         ,'data_source':'data_source'
                        }
    # add colmap_artg_entry_dict
    colmap_dict['artg_entry'] = colmap_artg_entry_dict


    ##### prepare manufacturers table #############
    # create manufacturers paths
    manufacturers = ['/Results/#/LicenceId'
                    ,'/Results/#/Manufacturers/#/Address/AddressLine1'
                    ,'/Results/#/Sponsor/Address/AddressLine2'
                    ,'/Results/#/Manufacturers/#/Address/Country'
                    ,'/Results/#/Manufacturers/#/Address/Postcode'
                    ,'/Results/#/Manufacturers/#/Address/State'
                    ,'/Results/#/Manufacturers/#/Address/Suburb'
                    ,'/Results/#/Manufacturers/#/Name']
    # add manufacturers paths
    paths_dict['manufacturers'] = manufacturers
    # create manufacturers colmap
    colmap_manufacturers_dict = {'LicenceId':'LicenceId'
                                 ,'AddressLine1':'AddressLine1'
                                 ,'AddressLine2':'AddressLine2'
                                 ,'Country':'Country'
                                 ,'Postcode':'Postcode'
                                 ,'State':'State'
                                 ,'Suburb':'Suburb'
                                 ,'Name':'Name'
                                 }
    # add manufacturers colmap
    colmap_dict['manufacturers'] = colmap_manufacturers_dict

    ##### prepare sponsor table #############
    # create products paths
    products = ['/Results/#/LicenceId'
               ,'/Results/#/Products/#/EffectiveDate'\
               ,'/Results/#/Products/#/GMDNCode'\
               ,'/Results/#/Products/#/GMDNTerm'\
               ,'/Results/#/Products/#/Name'\
               ,'/Results/#/Products/#/Type'\
              ]
    # add products paths
    paths_dict['products'] = products
    #  create products colmap
    colmap_products_dict = {'LicenceId':'LicenceId'
                             ,'EffectiveDate':'EffectiveDate'
                             ,'GMDNCode':'GMDNCode'
                             ,'GMDNTerm':'GMDNTerm'
                             ,'Name':'Name'
                             ,'Type':'Type'
                             }


    ##### prepare sponsor table #############
    # create sponsor paths
    sponsor = ['/Results/#/LicenceId'
            ,'/Results/#/Sponsor/Address/AddressLine1'
            ,'/Results/#/Sponsor/Address/AddressLine2'
            ,'/Results/#/Sponsor/Address/Country'
            ,'/Results/#/Sponsor/Address/Postcode'
            ,'/Results/#/Sponsor/Address/State'
            ,'/Results/#/Sponsor/Address/Suburb'
            ,'/Results/#/Sponsor/Name']
    # add sponsor path
    paths_dict['sponsor'] = sponsor
    # create sponsor colmap
    colmap_sponsor_dict = {'LicenceId':'LicenceId'
                            ,'AddressLine1':'AddressLine1'
                            ,'AddressLine2':'AddressLine2'
                            ,'Country':'Country'
                            ,'Postcode':'Postcode'
                            ,'State':'State'
                            ,'Suburb':'Suburb'
                            ,'Name':'Name'
                        }
    # add sponsor colmap
    colmap_dict['sponsor'] = colmap_sponsor_dict

    #source: http://apps.tga.gov.au/prod/ARTGSearch/ARTGWebService.svc/json/ARTGValueSearch/?entrytype=device&pagestart=1&pageend=1
    #path = r"C:\Users\60145210\Documents\ARTG\test2.json"
    #data = json.load(open(path))
    # for k,v in data.items():
    #     print("{} type {}".format(k,type(v)))
    #print('---------------')

    tab_with_records = get_tables_with_record(data,paths_dict,data_injection_lod = data_injection_lod)

    # inject timestamp
    ##### test debug here #######
    # for tab in tab_with_records:
    #     print(tab,tab_with_records[tab])
    # for k,v in colmap_dict.items():
    #     print(k,':',v)
    #     for src,trg in v.items():
    #         print(src,trg)
    #tab_with_records['artg_entry']['created_on'] = timestamp
    # print(tab_with_records['artg_entry'])
    # print('hello')
    # print(timestamp)


    ######
    ###### start interfacing to copython
    ######
    # create a CopyConf object with None arg (no config file passed in) and fill up its attributes
    cc = copyconf.CopyConf()
    cc.description = "copy a list of dictionary (LOD) into mssql"      # description of this copy

    # for each table create an instance of copypthon copy
    for tab in tab_with_records:
        #print(tab,tab_with_records[tab])

        # create a Copy object and define its source/target type
        c = copyconf.Copy("artg_entry_copy")
        c.source_type = "lod"
        c.target_type = "sql_table"

        # create a source object (in this case a csv object) and fill up its attributes
        src_obj = copyconf.LODConf()
        src_obj.lod = tab_with_records[tab]

        # assign this object to copy object as source
        c.source =src_obj


        # creat target object (in this case a sql table)
        trg_obj = copyconf.SQLTableConf()
        trg_obj.conn_str = "DRIVER={ODBC Driver 13 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=ARTG;UID=tester1;PWD=password;"
        trg_obj.schema_name = "dbo"
        trg_obj.table_name = tab
        # assign this object to copy object as target
        c.target = trg_obj

        # define column mapping.
        # in this simple example we need to create colmap objects and add them to the copy
        # of course there is better way to do this eg. get a list fo column mapping pair
        # and create a for-loop process to add any colmap.
        # colmap = copyconf.ColMapConf("EntryType","EntryType")
        # c.colmap_list.append(colmap)
        # colmap = copyconf.ColMapConf("LicenceClass","LicenceClass")
        # c.colmap_list.append(colmap)
        # colmap = copyconf.ColMapConf("LicenceId","LicenceId")
        # c.colmap_list.append(colmap)

        for k,v in colmap_dict.items():
            if k == tab:
                for src,trg in v.items():
                    colmap = copyconf.ColMapConf(src,trg)
                    c.colmap_list.append(colmap)


        #add this c (Copy instance) into cc (a CopyConf above)
        cc.add_copy(c)

    #----------------
    # end
    #----------------

    # call copython.copy_data and pass the cc as argument
    PRINT_RESULT = False
    res = copython.copy_data(cc,debug=False)
    if PRINT_RESULT:
        print("res={}".format(res))
