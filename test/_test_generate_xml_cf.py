"""
to improve my testing productivity, i need to have a way
to obtain info whether I'm testing against the installed package or 
directly from copython working folder (the one that I'm working with).
"""
import sys
sys.path.insert(0,r"E:\Documents\Visual Studio 2017\Projects\copython")

import copython
print(copython.__file__)
if __name__ == "__main__":
    conn_str = "DRIVER={ODBC Driver 11 for SQL Server};SERVER=LATITUDE;PORT=1443;DATABASE=GTFS;UID=user_name;PWD=password;"
    table_dict = {}
    table_dict["schema_name"]= "dbo"
    table_dict["table_name"]= "seq_routes"
    target_path = r"E:\Documents\Visual Studio 2017\Projects\copython\test\_cf_template_{}_{}.xml".format(table_dict["schema_name"],table_dict["table_name"])
    copython.gen_xml_cf_template(target_path,"csv","sql_table",conn_str,table_dict)