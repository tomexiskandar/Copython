use_package = False
if use_package is False:
    import sys
    sys.path.insert(0,r"C:\Users\60145210\Documents\Projects\copython")

import copython
from copython import copyconf
from copython import jsontabs
import app_test
import copy

def process(json_data,jsontabs_cf_path,data_injection_lod = None):

    jt = jsontabs.JSON_Tables(json_data,jsontabs_cf_path,data_injection_lod = data_injection_lod)

    # for tab in jt.tables:
    #     print('')
    #     print(tab.name)
    #     # print(' referencing cols:',tab.referencing_columns)
    #     # print(' cols tocopy:',tab.columns_tocopy)
    #     print(' ',tab.base_path)
    #     # if len(tab.child_table_list ) > 0:
    #     #     print(' ',tab.child_table_list)
    # quit()


    # print('\n--- debugging CURRENT ROW .....')
    # for tab in jt.tables:
    #     print(tab.name)
    #     print('-----------')
    #     print(' ',[str(x) for x in tab.current_row])
    #     print('-----------')
    #     # for i,row in enumerate(tab.rows):
    #     #     print(' ',i,row)
    # print('--- end debugging CURRENT ROW .....\n')
    #
    # print('\n--- debugging FINAL ROWS')
    # for tab in jt.tables:
    #     print(tab.name)
    #     print('-----------')
    #     # print(' ',tab.rows)
    #     for k,v in tab.rows.items():
    #         print(' ',k,v.print_kv())
    #     print('-----------')
    #     # if tab.name == 'prod__components':
    #     #     for k,v in tab.rows.items():
    #     #         print(' ',k,v.print_all_dc())
    #
    #
    # print('--- end debugging FINAL ROWS....\n')
    #quit()





    ######
    ###### start interfacing to copython
    ######
    # create a CopyConf object with None arg (no config file passed in) and fill up its attributes
    cc = copyconf.CopyConf()
    cc.description = "copy a flytab into mssql"      # description of this copy

    # for each table create an instance of copypthon copy
    for tab in jt.tables:
        # print(tab.name)
        # print(len(tab.rows))
        if len(tab.rows) == 0:
            continue

        # create a Copy object and define its source/target type
        c = copyconf.Copy("results_artg_copy")
        c.source_type = "flytab"
        c.target_type = "sql_table"

        # create a source object (in this case a csv object) and fill up its attributes
        src_obj = copyconf.FlyTableConf()
        src_obj.flytab = tab

        # assign this object to copy object as source
        c.source =src_obj


        # creat target object (in this case a sql table)
        trg_obj = copyconf.SQLTableConf()
        trg_obj.conn_str = "DRIVER={ODBC Driver 13 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=ARTG;UID=tester1;PWD=password;"
        trg_obj.schema_name = "artg"
        trg_obj.table_name = tab.name
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

        for src,trg in tab.colmap.items():
            #print('src',src,'trg',trg)
            colmap = copyconf.ColMapConf(src,trg)
            c.colmap_list.append(colmap)
        # colmap = copyconf.ColMapConf("Results","Results")
        # c.colmap_list.append(colmap)


        #add this c (Copy instance) into cc (a CopyConf above)
        cc.add_copy(c)

    #----------------
    # end
    #----------------

    # call copython.copy_data and pass the cc as argument
    PRINT_RESULT = False
    res = copython.copy_data(cc,debug=False,multi_process=True)
    if PRINT_RESULT:
        print("res={}".format(res))
