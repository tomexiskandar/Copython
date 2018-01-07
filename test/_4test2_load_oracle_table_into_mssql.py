"""
to improve my testing productivity, i need to have a way
to obtain info whether I'm testing against my installed copython package or 
directly from copython working folder (the one that I'm working with).
You should remove the two lines below when you test this script with your pc.
"""
import sys
sys.path.insert(0,r"e:\documents\visual studio 2017\projects\copython")


import copython

if __name__=="__main__":
    # drop target table (or comment the two lines below to append data into an existing table)
    conn_str = "DRIVER={ODBC Driver 11 for SQL Server};SERVER=LATITUDE;PORT=1443;DATABASE=Test;UID=user_name;PWD=password;"
    copython.drop_table(conn_str,"dbo","countries")
    
    config_path = r"E:\Documents\Visual Studio 2017\Projects\copython\test\_cf_load_oracle_table_into_mssql.xml"
    # call copython.copy_data
    res = copython.copy_data(config_path,debug=True)
    print("res={}".format(res))


