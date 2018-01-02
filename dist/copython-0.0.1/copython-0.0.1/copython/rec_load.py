import pyodbc
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
                    self.cursor.execute(batch_stmt)
                    self.conn.commit()
                    self.record_list.clear()

    def gen_row_param_markers(self,num_col,num_row):
        #print(num_col)
        #print(num_row)
        #print("the param lenght is -----> {}".format(str(num_col*num_row)))
        param = "(" + ",".join("?"*num_col) + ")"
        params = []
        for i in range(num_row):
            params.append(param)
        return ",".join(params)
