"""
To improve my testing productivity, i need to organise all testing files in test folder.
I create var use_package to control what kind of codes i'm testing, for eg.
if the value is False, I'm executing copython codes from my working folder, while True
I'm executing copython codes from the site_packages
You should remove the four lines below if you have to.
"""
use_package = False
if use_package is False:
    import sys
    sys.path.insert(0,r"e:\documents\visual studio 2017\projects\copython")



"""
csv sample data is the one of the gtfs files format belong to Queensland Government Australia.
I download the file from the following website.
https://gtfsrt.api.translink.com.au/
"""


import copython
from copython import copyconf

if __name__=="__main__":
    # drop target table (or comment the two lines below to append data into an existing table)
    conn_str = "DRIVER={ODBC Driver 11 for SQL Server};SERVER=LATITUDE;PORT=1443;DATABASE=Test;UID=user_name;PWD=password;"
    copython.drop_table(conn_str,"dbo","routes")

    #----------------------------------------
    # start writing a simple programmable copy
    #----------------------------------------
    
    # create a CopyConf object with None arg (no config file passed in) and fill up its attributes
    cc = copyconf.CopyConf(None)
    cc.description = "copy csv file into mssql"      # description of this copy

    # create a Copy object and define its source/target type
    c = copyconf.Copy("routes")
    c.source_type = "csv"
    c.target_type = "sql_table"

    # create a source object (in this case a csv object) and fill up its attributes
    src_obj = copyconf.CSVConf()
    src_obj.path = r"E:\Documents\Visual Studio 2017\Projects\copython\test\routes.txt"
    src_obj.encoding = "utf-8-sig"
    src_obj.has_header = "yes" 
    src_obj.delimiter = ","
    src_obj.quotechar = '"' # or "\""
    # assign this object to copy object as source
    c.source =src_obj

    # creat target object (in this case a sql table)
    trg_obj = copyconf.SQLTableConf()
    trg_obj.conn_str = "DRIVER={ODBC Driver 11 for SQL Server};SERVER=LATITUDE;PORT=1443;DATABASE=Test;UID=user_name;PWD=password;"
    trg_obj.schema_name = "dbo"
    trg_obj.table_name = "routes"
    # assign this object to copy object as target
    c.target = trg_obj

    # define column mapping. 
    # in this simple example we need to create colmap objects and add them to the copy
    # of course there is better way to do this eg. get table's columns from database
    # and create a for-loop process to add any colmap.
    colmap = copyconf.ColMapConf("route_id","route_id")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("route_short_name","route_short_name")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("route_long_name","route_long_name")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("route_desc","route_desc")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("route_type","route_type")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("route_url","route_url")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("route_color","route_color")
    c.colmap_list.append(colmap)
    colmap = copyconf.ColMapConf("route_text_color","route_text_color")
    c.colmap_list.append(colmap)
    # add this c (Copy instance) into cc (a CopyConf above)
    cc.add_copy(c)

    #----------------
    # end
    #----------------

    # call copython.copy_data and pass the cc as argument
    res = copython.copy_data(cc,debug=True)
    print("res={}".format(res))


