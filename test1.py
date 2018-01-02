import copython

if __name__=="__main__":

    
    conn_str = "DRIVER={ODBC Driver 11 for SQL Server};SERVER=LATITUDE;PORT=1443;DATABASE=GTFS;UID=user_name;PWD=password;"
    copython.drop_table(conn_str,"dbo","seq_routes")
    
    config_path = r"E:\Documents\Visual Studio 2017\Projects\copython\test\_test_cf_load_csv_tab_into_mssql.xml"
    res = copython.copy_data(config_path,debug=True)
    print(res)