import csv
import pyodbc
def record_generator(metadata,mapped_column_name_list,copy):
    if metadata.__class__.__name__ == "CSVMetadata":
        with open(metadata.path, 'r', encoding=metadata.encoding) as csvfile:
            if metadata.has_header is True:
                next(csvfile)
            reader = csv.reader(csvfile,delimiter=metadata.delimiter,quotechar='"')
            # print("")
            #print(mapped_column_name_list)
            #quit()
            csv_column_name_list = [x.column_name for x in metadata.column_list]
            #print(csv_column_name_list)
      
            
            
            
            for line in reader:
                mapped_column_data_list = []
                for col, val in zip(mapped_column_name_list, line):
                    if col in mapped_column_name_list:
                        mapped_column_data_list.append(val)
                yield(mapped_column_data_list)

    elif metadata.__class__.__name__ == "SQLTableMetadata":
        conn = pyodbc.connect(str(metadata.conn_str))
        sql = "SELECT * FROM {}.{}".format(metadata.schema_name,metadata.table_name)
        cursor = conn.cursor()
        for row in cursor.execute(sql).fetchall():
            yield(list(row))

    elif metadata.__class__.__name__ == "SQLQueryMetadata":
        conn = pyodbc.connect(str(metadata.conn_str))
        sql = metadata.sql_str
        cursor = conn.cursor()
        for row in cursor.execute(sql).fetchall():
            yield(list(row))

    elif metadata.__class__.__name__ == "LODMetadata":
        records = []
        for dict in metadata.lod:
            record = []
            for k,v in dict.items():
                record.append(v)
            records.append(record)
        for row in records:
            #print(row)
            yield row

    elif metadata.__class__.__name__ == "BinTableMetadata":
        # print("column---->",metadata.column_name_list)
        # print(mapped_column_name_list)
       
        for rowid,row in metadata.bin_table.iterrows():
            # print(row)
            # quit()
            record = []
            # populate record depending whether sql table exists or not
            # use colmap is table exist otherwise ignore the colmap
            if copy.target.has_sql_table:
                for colname in mapped_column_name_list:
                    # iterate colmap list
                    for colmap in copy.colmap_list:   
                        # check if the colname equals the target
                        if colname == colmap.target:
                            record.append(row[colmap.source])
            else:
                for colname in mapped_column_name_list:
                    record.append(row[colname])                

            # for mappedcolname in mapped_column_name_list:
            #     record.append(row[mappedcolname])
            # print("record---->",record)
            yield record
