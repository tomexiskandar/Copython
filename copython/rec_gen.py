import csv
import pyodbc
def record_generator(metadata,mapped_column_name_list,copy_colmap_list):
    if metadata.__class__.__name__ == "CSVMetadata":
        with open(metadata.path, 'r', encoding=metadata.encoding) as csvfile:
            if metadata.has_header is True:
                next(csvfile)
            reader = csv.reader(csvfile,delimiter=metadata.delimiter,quotechar='"')
            # print("")
            # print(mapped_column_name_list)
            csv_column_name_list = [x.column_name for x in metadata.column_list]
            # print(csv_column_name_list)
            
            
            
            for line in reader:
                mapped_column_data_list = []
                for col, val in zip(csv_column_name_list, line):
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

    elif metadata.__class__.__name__ == "FlyTableMetadata":
        #print("column---->",metadata.column_name_list)
        for rowid,row in metadata.flytab.rows.items():
            #print(rowid,row)
            record = []
            for mappedcolname in mapped_column_name_list:
                # iterate colmap list
                for colmap in copy_colmap_list:
                    # check if the mappedcolname equals the target
                    if mappedcolname == colmap.target:
                        # if yes then retrieve the dc.value by its source
                        for dc in row.datarow.values():
                            if dc.column_name == colmap.source:
                                record.append(dc.value)
                                break

                # for dc in row.datarow:
                #     for colmap in copy_colmap_list:
                #         if dc.column_name == colmap:
                #             record.append(dc.value)
            #print("record---->",record)
            yield record
