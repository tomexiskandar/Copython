
use_package = False
if use_package is False:
    import sys
    sys.path.insert(0,r"C:\Users\60145210\Documents\Projects\copython")



"""
csv sample data is the one of the gtfs files format belong to Queensland Government Australia.
I download the file from the following website.
https://gtfsrt.api.translink.com.au/
"""


import copython

if __name__=="__main__":
    # set config file path
    config_path = r"E:\copython_samples\config_load_csv_into_mssql.json"
    # call copython.copy_data
    res = copython.copy_data(config_path,debug=False)
    # check error if any
    print("res={}".format(res))
