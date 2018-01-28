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

import copython
from copython import copyconf

#print(copython.__file__)
if __name__ == "__main__":

    # use config class
    csv_conf = copyconf.CSVConf()
    csv_conf.path = r"E:\DATA\SEQ_GTFS\shapes.txt"
    csv_conf.delimiter = ","
    csv_conf.encoding = "utf-8-sig"
    csv_conf.has_header = "yes"
    csv_conf.quotechar = '"'

    #sql_query = copyconf.SQLQueryConf()
    #sql_query.conn_str = "DRIVER={ODBC Driver 11 for SQL Server};SERVER=LATITUDE;PORT=1443;DATABASE=Test;UID=user_name;PWD=password;"
    #sql_query.sql_str = "SELECT * FROM countries"

    #sql_tab_conf1 = copyconf.SQLTableConf()
    #sql_tab_conf1.conn_str = "DRIVER={PostgreSQL Unicode(x64)};SERVER=localhost;PORT=5432;DATABASE=Test;UID=user_name;PWD=password"
    #sql_tab_conf1.schema_name = "public"
    #sql_tab_conf1.table_name = "agency"
  
    sql_tab_conf = copyconf.SQLTableConf()
    sql_tab_conf.conn_str = "DRIVER={ODBC Driver 11 for SQL Server};SERVER=LATITUDE;PORT=1443;DATABASE=Test;UID=user_name;PWD=password;"
    sql_tab_conf.schema_name = "dbo"
    sql_tab_conf.table_name = "shapes"
 
    output_path = r"E:\Documents\Visual Studio 2017\Projects\copython\test\_cf_template_{}_{}.xml".format(sql_tab_conf.schema_name,sql_tab_conf.table_name)
    copython.gen_xml_cf_template(output_path,csv_conf,sql_tab_conf,"source")