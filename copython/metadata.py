import csv
import pyodbc


class SQLDescCol:
    def __init__(self):
        self.column_name = None
        self.type_code = None
        self.display_size = None
        self.internal_size = None
        self.precision = None
        self.scale = None
        self.nullable = None

class TinyTable():
    def __init__(self,sql_table,conn_str):
        self.sql_table = sql_table
        self.conn_str = conn_str
        self.column_list = []
        self.record_list = []
        self.gen_column_record_list()
    def gen_column_record_list(self):
        conn = pyodbc.connect(str(self.conn_str))
        cursor = conn.cursor()
        rows = cursor.columns(table = self.sql_table.table_name,schema = self.sql_table.schema_name).fetchall()
        cols = cursor.description
        self.set_column_list(cols)
        self.set_record_list(rows)
    def set_column_list(self,columns):
        for col in columns:
            desc_col = SQLDescCol()
            for i,k in enumerate(desc_col.__dict__.keys()):
                if i < len(col):
                    setattr(desc_col,k,col[i])
            self.column_list.append(desc_col)
    def set_record_list(self,records):
        for record in records:
            self.record_list.append(record)
    def print(self,column_name_list = 0):
        if column_name_list == 0:
            column_name_list = [x.column_name for x in self.column_list]
            for record in self.record_list:
                zipped = dict(zip(column_name_list,record))
                print(zipped)
        else:
            _cols = column_name_list.split(",")
            _idx = []
            for col_name in _cols:
                index = next((i for i, item in enumerate(self.column_list) if item.column_name == col_name), -1)
                if index == -1:
                    raise Exception ("couldn't find column name {}".format(col_name))
                _idx.append(index)
            for record in self.record_list:
                _new_record = []
                for i in _idx:
                    _new_record.append(record[i])
                zipped = dict(zip(_cols,_new_record))
                print(zipped)


class Column: #describe column object
    def __init__(self):
        self.table_cat = None
        self.table_schem = None
        self.table_name = None
        self.column_name = None
        self.data_type = None
        self.type_name = None
        self.column_size = None
        self.buffer_length = None
        self.decimal_digits = None
        self.num_prec_radix = None
        self.nullable = None
        self.remarks = None
        self.column_def = None
        self.sql_data_type = None
        self.sql_datetime_sub = None
        self.char_octet_length = None
        self.ordinal_position = None
        self.is_nullable = None
        self.ss_is_sparse = None

class SQLDataType:#describe datatype_info
    def __init__(self):
        self.type_name = None
        self.data_type= None
        self.column_size= None
        self.literal_prefix= None
        self.literal_suffix= None
        self.create_params= None
        self.nullable= None
        self.case_sensitive= None
        self.searchable= None
        self.unsigned_attribute= None
        self.fixed_prec_scale= None
        self.auto_unique_value= None
        self.local_type_name= None
        self.minimum_scale= None
        self.maximum_scale= None
        self.sql_data_type= None
        self.sql_datetime_sub= None
        self.num_prec_radix= None
        self.interval_precision= None

class SQLTypeInfoMap:#the list of datatype info
    def __init__(self):
        self.src_type_info_list = []
        self.trg_type_info_list = []
        #self.set_list(conn_str)
        #### get sql type info list
    def set_list(self,end_point,end_point_name):
        #if end_point_name == "source":
        #    self.src_type_info_list = None
        #    return
        conn = pyodbc.connect(str(end_point.conn_str)) 
        cursor = conn.cursor()
       
        sql_type_info_tuple = cursor.getTypeInfo(sqlType = None)
        #print(sql_type_info_tuple)
        #for row in sql_type_info_tuple:
        #    print(row)
        #quit()
        
        for row in sql_type_info_tuple:
            _sql_type_info = SQLDataType()
            for i,k in enumerate(_sql_type_info.__dict__.keys()):
                #get attribut name by index instead the dict keys
                #due to column name changes from one odbc version to another
                #this line is not good if version change->setattr(_sql_type_info,k,getattr(row,k))
                setattr(_sql_type_info,k,row[i])
            if end_point_name == "source":
                self.src_type_info_list.append(_sql_type_info)
            if end_point_name == "target":
                self.trg_type_info_list.append(_sql_type_info)
    
    def get_target_type_name(self,data_type):
        index = next((i for i, item in enumerate(self.trg_type_info_list) if item.data_type == data_type), -1)
        if index > -1:
            return getattr(self.trg_type_info_list[index],"type_name")

    def get_target_info(self,type_name,searched_info = 0):
        index = next((i for i, item in enumerate(self.trg_type_info_list) if item.type_name == type_name), -1)
        #print(index)
        #quit()
        if index > -1:
            if searched_info != 0:
                return getattr(self.trg_type_info_list[index],searched_info)
            else:
                row = self.trg_type_info_list[index]
                return row.__dict__
        else:
            return None

    def get_type_name_for_csv(self):
        #!!! this is not right. it must use data type abstraction
        res = self.get_info("nvarchar","data_type")
        if res == -9:
            return "nvarchar"
        if res is None:
            res2 = self.get_info("varchar","data_type")
            if res2 == 12:
                return "varchar"

class CSVMetadata:
    """this class store metadata for a single table.
    """
    def __init__(self,end_point): 
        #data members
        self.path = end_point.path
        self.has_header = end_point.has_header
        self.encoding = end_point.encoding.lower()
        self.delimiter = end_point.delimiter
        self.quotechar = end_point.quotechar
        self.column_list = self.get_csv_column_list(end_point)
        self.set_data_type()

    def get_csv_column_list(self,end_point,inspect_col_size = 0):
        with open(end_point.path, 'r', encoding=end_point.encoding) as csvfile:
            reader = csv.reader(csvfile,delimiter=end_point.delimiter,quotechar=end_point.quotechar)
            if end_point.has_header:
                #### add column_name to metadata
                column_name_list = next(reader)
                _column_list = []
                #### add columns in column list. must suss out the sqltypeinfo of the target table
                for _col_name in column_name_list:
                    _col = Column()
                    _col.column_name = _col_name
                    _column_list.append(_col)  
            return _column_list

    def set_data_type(self):
        #set the type_name
        src_data_type = 0
        if self.encoding in ["utf-8","utf-8-sig"]:
            src_data_type = -9
        else:
            src_data_type = 12

        #then update the src_md
        for col in self.column_list:
            col.data_type = src_data_type

    def set_csv_column_size(self):
        with open(self.path, 'r', encoding=self.encoding) as csvfile:
            reader = csv.reader(csvfile,delimiter=self.delimiter,quotechar=self.quotechar)
            if self.has_header:
                next(reader)
                
           #### inspect column_size
            for col in self.column_list:#re-initialise column_size
                col.column_size = 1
        
            for i,line in enumerate(reader,1):
                for j,v in enumerate(line):
                    #print(_csv_md.column_list[j].column_size)
                    if len(v) > self.column_list[j].column_size:
                        self.column_list[j].column_size = len(v)
    
class SQLTableMetadata:
    """An instance created for a server's table metadata.
    """
    def __init__(self,end_point): 
        #data members
        self.conn_str = None
        self.server_name = None #from config
        self.db_name = None   #
        self.schema_name = None #from config
        self.table_name = None #from config
        self.column_list = []
        self.get_sql_table_metadata(end_point)
    def get_sql_table_metadata(self,end_point):
        conn = pyodbc.connect(str(end_point.conn_str)) 
        cursor = conn.cursor()
        _col_tuple = cursor.columns(table=end_point.table_name,schema=end_point.schema_name).fetchall()
        #for col in _col_tuple:
        #    print (col)
        self.conn_str = end_point.conn_str
        #### extract server name, db_name, table schema and table name from the connection and tuple
        self.server_name = conn.getinfo(pyodbc.SQL_SERVER_NAME)
        self.db_name = _col_tuple[0][0]
        self.schema_name = end_point.schema_name # assign value from user
        self.table_name = end_point.table_name   # assign value from user
        #### populate columns into column_list
        for row in _col_tuple:
            _col = Column()
            for i,k in enumerate(_col.__dict__.keys()):
                #set attributes by index instead the keys name
                #setattr(_col,k,getattr(row,k))
                #print("-------------")
                #print(k,row[i])
                if i < len(row):
                    setattr(_col,k,row[i])
            self.column_list.append(_col)
           
class SQLQueryMetadata:
    """An instance created for metadata from an sql query
    """
    def __init__(self,end_point): 
        #data members
        self.conn_str = None
        self.server_name = None #from config
        self.db_name = None   #
        self.sql_str = None
        self.column_list = []
        self.get_sql_query_metadata(end_point)

    def get_sql_query_metadata(self,end_point):
        conn = pyodbc.connect(str(end_point.conn_str)) 
        cursor = conn.cursor()
        row = cursor.execute(end_point.sql_str).fetchone()#fetch one just for a dummy executing as we need columns
        _desc_tuple = cursor.description
        #for col in _desc_tuple:
        #    print(col)
        self.conn_str = end_point.conn_str
        #### extract server name, db_name, table schema and table name from the connection and tuple
        self.server_name = conn.getinfo(pyodbc.SQL_SERVER_NAME)
        self.db_name = conn.getinfo(pyodbc.SQL_DATABASE_NAME)#_col_tuple[0][0]
        self.sql_str = end_point.sql_str
        #### populate columns into column_list
        #### and determine data_type (the integer value data_type as per odbc)
        for row in _desc_tuple:
            _col = SQLDescCol()
            _col_copy = Column()
            for i,k in enumerate(_col.__dict__.keys()):
                #set attributes by index instead the keys name
                #setattr(_col,k,getattr(row,k))
                #print("-------------")
                #print(k,row[i])
                if i < len(row):
                    #if conversion from SQLDescCol() to Column() is required
                    if k == "type_code":
                        setattr(_col_copy,"data_type",type_map[row[i].__name__])
                    if k == "precision":
                        setattr(_col_copy,"column_size",row[i])
                    if k == "scale":
                        setattr(_col_copy,"decimal_digits",row[i])
                    #if conversion is not required
                    else:
                        setattr(_col_copy,k,row[i])
            self.column_list.append(_col_copy) 
            
def is_sql_table_existence(end_point):
    conn = pyodbc.connect(str(end_point.conn_str)) 
    cursor = conn.cursor()
    if cursor.tables(schema=end_point.schema_name,table=end_point.table_name,tableType="TABLE").fetchone():
        return True
    else:
        return False

def create_simple_sql_table(copy_target,type_info_map,src_md,copy_optional):
    """
    copy_target provides conn to the target while
    src_md provides metadata required to build up columns
    """
        
    #for col in src_md.column_list:
    #    print(col.column_name,col.column_size)
    
    #if src_md is a CSVMetadata then complete a few of column properties
    if src_md.__class__.__name__ == "CSVMetadata":
        #set column size
        src_md.set_csv_column_size()

    create_table_col_list = []
    for src_col in src_md.column_list:
        _create_col_list = []
        _create_col_list.append('"{}"'.format(src_col.column_name))
        _trg_type_name = type_info_map.get_target_type_name(src_col.data_type)
        _create_col_list.append(_trg_type_name) 
        create_param = type_info_map.get_target_info(_trg_type_name,"create_params")
        if create_param is not None:
            create_param_list = create_param.split(",")
            #print(create_para_list)
        #if create_param_list is not None:
            _create_col_list.append("(")
            if create_param in ["length","max length","max. length"]:
                _create_col_list.append(str(src_col.column_size))
            if create_param  == "precision,scale":
                 _create_col_list.append(str(src_col.column_size) + "," + str(src_col.decimal_digits))
            if create_param  == "scale":
                 _create_col_list.append(str(src_col.decimal_digits))

            _create_col_list.append(")")
        create_table_col_list.append(" ".join(_create_col_list))
    #print(create_table_col_list)

       
    conn = pyodbc.connect(str(copy_target.conn_str)) 
    cursor = conn.cursor()

    create_tbl_stmt = "CREATE TABLE {}.{} ({})".format(copy_target.schema_name,copy_target.table_name,",".join(create_table_col_list))
            
    if copy_optional['debug']:
        print(create_tbl_stmt)  
    cursor.execute(create_tbl_stmt)
    conn.commit()
    if copy_optional['debug']:
        print("table {}.{} created".format(copy_target.schema_name,copy_target.table_name))

type_map = {'bool':-7,'bytes':-3
            ,'Decimal':3,'long':4,'float':6
            ,'str':12,'time':93,'date':93,'datetime':93}

