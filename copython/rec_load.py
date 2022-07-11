import os
import re
from datetime import datetime
import pyodbc

class Dump:
    """plain data dump"""
    def __init__(self,dirname,data, name=None):
        self.dirname = dirname
        self.data = data
        self.name = name
        #self.totext(dirname,data,name)
    def totext(self):
        timestamp = str(datetime.now())
        timestamp = re.sub("[-:.]","",timestamp)
        if self.name is not None:
            filename = "dump_" + self.name
        else:
            filename = "dump"
        path = os.path.join(self.dirname,filename + timestamp + ".txt")
        try:
            file = open(path,"w")
            if type(self.data) is list or type(self.data) is tuple:
                for row in self.data:
                    try:
                        file.write(str(row)+ '\n') #
                    except Exception as e:
                        print('warning! {} with reference to writing row {}.'.format(e,row))
            else:
                file.write(str(self.data))
        except Exception as e:
            print('warning! {} with reference to dumping object.'.format(e))
            pass

    def tocsv(self):
        timestamp = str(datetime.now())
        timestamp = re.sub("[-:.]","",timestamp)
        if self.name is not None:
            filename = "dump_" + self.name
        else:
            filename = "dump"
        path = os.path.join(self.dirname,filename + timestamp + ".csv")
        try:
            file = open(path,"w")
            if type(self.data) is list or type(self.data) is tuple:
                i = 0
                for row in self.data:
                    columnname_list = list(row.keys())
                    data_list = list(row.values())
                    try:
                        if i == 0: #columns and data
                            file.write(','.join(columnname_list) + '\n')
                            file.write(','.join(str(x) for x in data_list) + '\n')
                        if i > 0: #data only
                            file.write(','.join(str(x) for x in data_list) + '\n')
                    except Exception as e:
                        print('warning! {} with reference to writing row {}.'.format(e,row))

                    i += 1
            else:
                file.write(str(self.data))
        except Exception as e:
            print('warning! {} with reference to dumping object.'.format(e))
            pass


class RecordLoader:
    #def __init__(self,cursor,target_table_dict,mapped_column_name_list,insert_method,row_count,max_row_per_batch):
    def __init__(self,copy_target,target_metadata,sql_record,insert_method,max_row_per_batch):
        self.conn = pyodbc.connect(str(copy_target.conn_str))
        self.cursor = self.conn.cursor()
        self.target_metadata = target_metadata
        self.max_row_per_batch = max_row_per_batch
        self.mapped_column_name_list = self.set_mapped_column_name_list(sql_record)
        self.insert_method = insert_method
        self.record_list = []
        self.param_marker = ""
        self.param_value_list = []

    def set_mapped_column_name_list(self,sql_record):
        return ['"{}"'.format(x) for x in sql_record.mapped_column_name_list]


    def add_record(self,record):
        if record:
            if self.insert_method == "prepared":
                for v in record:#row_param_value_list:
                    self.record_list.append(v)
            else:#reserved for batch
                self.record_list.append("(" + ",".join(record)+")")
        if self.insert_method == "prepared":
            batch_count = int(len(self.record_list)/len(self.mapped_column_name_list))
        else:#reserved for batch
            batch_count = len(self.record_list)

        if(batch_count % self.max_row_per_batch == 0 or record == False):
            #print("batch count {} len(record_list) {}".format(batch_count,len(self.record_list)))

            #len(self.record_list)/len(self.mapped_column_name_list)
            if len(self.record_list) > 0:
                if self.insert_method == "prepared":
                    if record:
                        _param_markers = self.gen_row_param_markers(len(self.mapped_column_name_list),self.max_row_per_batch)
                    else:
                        _param_markers = self.gen_row_param_markers(len(self.mapped_column_name_list),batch_count)
                    #print("----")
                    #print(_param_markers)
                    prepared_stmt = """INSERT INTO {}.{} ({}) VALUES {}
                                    """.format(self.target_metadata.schema_name,self.target_metadata.table_name,",".join(self.mapped_column_name_list),_param_markers)
                    #print(prepared_stmt)
                    #print(self.record_list)

                    self.cursor.execute(prepared_stmt,self.record_list)
                    self.conn.commit()
                    self.record_list.clear()
                else:#reserved for batch
                    batch_stmt = """INSERT INTO {}.{} ({}) VALUES {}
                                """.format(self.target_metadata.schema_name,self.target_metadata.table_name,",".join(self.mapped_column_name_list),",".join(self.record_list))

                    #print(batch_stmt)
                    # print(self.record_list)            
                    
                    try:
                        self.cursor.execute(batch_stmt)
                        self.conn.commit()
                        self.record_list.clear()
                    except Exception as e:
                        print(e)
                        print('failed!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                        cwd = os.getcwd()

                        # print(cwd)
                        timestamp = str(datetime.now())
                        timestamp = re.sub("[-:.]","",timestamp)
                        path = cwd + "\\dumps\\dump" + timestamp + ".txt"
                        print(path)
                        file = open(path,"w")
                        file.write(str(batch_stmt)+ '\n') #
                        quit()


    def gen_row_param_markers(self,num_col,num_row):
        #print(num_col)
        #print(num_row)
        #print("the param lenght is -----> {}".format(str(num_col*num_row)))
        param = "(" + ",".join("?"*num_col) + ")"
        params = []
        for i in range(num_row):
            params.append(param)
        return ",".join(params)
