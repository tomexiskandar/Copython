"""
To improve my testing productivity, i need to organise all testing files in test folder.
I create var use_package to control what kind of codes i'm testing, for eg.
if the value is False, I'm executing copython codes from my working folder, while True
I'm executing copython codes from the site_packages
You should remove the four lines below .
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

if __name__=="__main__":
    # drop target table (or comment the two lines below to append data into an existing table)
    conn_str = "DRIVER={ODBC Driver 11 for SQL Server};SERVER=LATITUDE;PORT=1443;DATABASE=Test;UID=user_name;PWD=password;"
    copython.drop_table(conn_str,"dbo","seq_routes")
    
    config_path = r"E:\Documents\Visual Studio 2017\Projects\copython\test\_cf_load_csv_into_mssql.xml"
    # call copython.copy_data
    res = copython.copy_data(config_path,debug=True)
    print("res={}".format(res))


