use_package = False
if use_package is False:
    import sys
    sys.path.insert(0,r"C:\Users\60145210\Documents\Projects\copython")

import copython
from copython import copyconf
from copython import jsontabs
import app_test


def process(data,session_dt_string = None):
    # print(data)
    # print(session_dt_string)
    # drop target table (or comment the two lines below to append data into an existing table)
    #conn_str = "DRIVER={ODBC Driver 13 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=ARTG;UID=tester1;PWD=password;"
    #copython.drop_table(conn_str,"dbo","artg_entry")

    #----------------------------------------
    # start writing a simple programmable copy
    #----------------------------------------
    path = r"C:\Users\60145210\Documents\Projects\copython\test\artg_project\_cf_path_totable.json"
    jc = jsontabs.JSONPath_Tables_Conf(path)

    #define if there is any data injection required
    data_injection_lod = []
    data_injection_lod.append({"table_name":"artg_entry","column_name":"created_on","value":session_dt_string})
    data_injection_lod.append({"table_name":"artg_entry","column_name":"data_source","value":"apps.tga.gov.au"})
    #print(jc.jsondata)
    #jc.set_intertables_column_copies()
    #print(jc.intertables_column_copies)
    #jc.set_referencing_columns()
    # for tab in jc.tables:
    #     print(tab.name)
    #     print(' ',tab.referencing_columns)
    #     print(' ',tab.referencing_tables)
    # quit()
    # for tab in jc.tables:
    #     print(' tab', tab.name)
    #     print(' isprimary', tab.is_primary)
    #
    # quit(0)


    #print('---start debugging...')
    # traverse the entire key & value and any sub key & value
    jc = jsontabs.gen_json_tables(jc,data,data_injection_lod = data_injection_lod)



    # print('\n--- debugging CURRENT ROW .....')
    # for tab in jc.tables:
    #     print(tab.name)
    #     print('-----------')
    #     print(' ',[str(x) for x in tab.current_row])
    #     print('-----------')
    #     # for i,row in enumerate(tab.rows):
    #     #     print(' ',i,row)
    # print('--- end debugging CURRENT ROW .....\n')

    # print('--- debugging FINAL ROWS')
    # for tab in jc.tables:
    #     print(tab.name)
    #     print('-----------')
    #     #print(' ',tab.rows)
    #     for k,v in tab.rows.items():
    #         print(' ',k,v.print_kv())
    #     print('-----------')
    #
    # print('--- end debugging FINAL ROWS....\n')
    #
    # quit()


    # for tab in jc.tables:
    #     print(tab.name)
    #     print(' ',len(tab.rows))
    #     print(' counter:',tab.counter)
    # for rowid,row in jc.tables[0].rows.items():
    #     print(rowid,row)
    #     for dc in row.datarow:
    #         print('--->',dc)






    ##### test debug here #######
    # for tab in jc.tables:
    #     print(tab.name)
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
    cc.description = "copy a flytab into mssql"      # description of this copy

    # for each table create an instance of copypthon copy
    for tab in jc.tables:
        print(tab.name)

        # create a Copy object and define its source/target type
        c = copyconf.Copy("artg_entry_copy")
        c.source_type = "flytab"
        c.target_type = "sql_table"

        # create a source object (in this case a csv object) and fill up its attributes
        src_obj = copyconf.FlyTableConf()
        src_obj.flytab = tab

        # assign this object to copy object as source
        c.source =src_obj


        # creat target object (in this case a sql table)
        trg_obj = copyconf.SQLTableConf()
        trg_obj.conn_str = "DRIVER={ODBC Driver 13 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=Test;UID=tester1;PWD=password;"
        trg_obj.schema_name = "dbo"
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
