"""
to improve my testing productivity, i need to have a way
to obtain info whether I'm testing against my installed copython package or 
directly from copython working folder (the one that I'm working with).
You should remove the two lines below when you test this script with your pc.
"""
import sys
sys.path.insert(0,r"e:\documents\visual studio 2017\projects\copython")



"""
csv sample data is the one of the gtfs files format belong to Queensland Government Australia.
I download the file from the following website.
https://gtfsrt.api.translink.com.au/
"""


import copython

if __name__=="__main__":
    # drop target table (or comment the two lines below to append data into an existing table)
    conn_str = "DRIVER={PostgreSQL Unicode(x64)};SERVER=localhost;PORT=5432;DATABASE=Bingy;UID=postgres;PWD=Bintang"
    copython.drop_table(conn_str,"public","seq_routes")
    
    config_path = r"E:\Documents\Visual Studio 2017\Projects\copython\test\_cf_load_csv_into_pgsql.xml"
    # call copython.copy_data
    res = copython.copy_data(config_path,debug=True)
    print("res={}".format(res))


