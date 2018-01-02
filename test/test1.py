"""
to improve code readability and quick testing in my environment,
I created a test folder for all/most testing scripts.
Since this folder out of the scope of the copython library, then
I have to append the project root directory into the sys.path in order
the statement "import copython" to work.
"""
import os,sys
sys.path.append('E:\Documents\Visual Studio 2017\Projects\copython')

import copython
print(copython.__file__)
if __name__=="__main__":

    
    conn_str = "DRIVER={ODBC Driver 11 for SQL Server};SERVER=LATITUDE;PORT=1443;DATABASE=Bingy;Trusted_Connection=yes;"
    copython.drop_table(conn_str,"dbo","nsw_agency")
    
    config_path = r"E:\Documents\Visual Studio 2017\Projects\copython\test\_test_cf_load_csv_tab_into_mssql.xml"
    res = copython.copy_data(config_path,debug=True)
    print(res)