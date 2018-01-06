"""
csv sample data is one of the gtfs files format belong to Queensland Government Australia.
I downloaded the file from the following website.
https://gtfsrt.api.translink.com.au/
"""


import copython
#is the copython from the site-packages or copython working folder
print(copython.__file__)


if __name__=="__main__":
    # drop target table (or comment the two lines below to append data)
    conn_str = "DRIVER={ODBC Driver 11 for SQL Server};SERVER=LATITUDE;PORT=1443;DATABASE=GTFS;UID=user_name;PWD=password"
    copython.drop_table(conn_str,"dbo","seq_routes")
    
    config_path = r"E:\Documents\Visual Studio 2017\Projects\copython\test\_test_cf_load_csv_tab_into_mssql.xml"
    # call copython.copy_data
    res = copython.copy_data(config_path,debug=True)
    print("res={}".format(res))