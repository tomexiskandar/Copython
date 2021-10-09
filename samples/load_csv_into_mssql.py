"""
csv sample data being used in config file belong to TransLink's open data.
https://gtfsrt.api.translink.com.au/
"""

import copython

if __name__=="__main__":
    # set config file path
    config_path = r"E:\copython_samples\config_load_csv_into_mssql.json"
    # call copython.copy_data
    res = copython.copy_data(config_path,debug=True)
    # check error if any
    print("res={}".format(res))
