"""
copython is an engine to copy data to and from sql database.
it defines functions to be called by client codes.
the arg is an xml config file. this file will be used to 
create/complete metadata for the source and target.
"""
import xml.etree.ElementTree as ET
import xml.dom.minidom
import pyodbc
import os
import sys
import io
from multiprocessing import Pool, freeze_support
import multiprocessing
import threading
import csv
import datetime
import time
import itertools


"""
folder copython
"""
from copython import copyconf
from copython import metadata
from copython import rec_gen
from copython import sql_rec
from copython import rec_load


"""
implementation
define interfaces for client's codes
"""

def copy_data(config, debug=False, insert_method='batch', multi_process=False):
    try:
        #optional parameters for config
        optional = {}
        optional['debug'] = debug
        optional["insert_method"] = insert_method
        #print(pyodbc.drivers())
        #quit()

        # evaluate the type of config, whether a CopyConf instance or config file (either xml or json)
        cc = None
        try:
            cc = config
        except:
            cc = copyconf.CopyConf(config)
        #for c in cc.copy_list:
        #    for k,v in c.__dict__.items():
        #        print(k,v)
        
        # validate config
        cc.validate_config_object() 
        
        # validate config for existence/accessible
        cc.validate()
        
        if debug:
            cc.debug()
    
        if multi_process is True:
            copies = []
            copies =  [c for c in cc.copy_list]
            for copy in copies:
                copy.optional = optional
                with Pool(processes=4) as pool:
                    p = pool.map(execute_copy,copies)
        else:
            for copy in cc.copy_list:
                copy.optional = optional
                execute_copy(copy)
        return 0
    except Exception as e: 
        return e
def execute_copy(copy):
    start = datetime.datetime.now() # hold starttime for duration
    spinner = itertools.cycle(['-', '\\', '|', '/']) # hold a list to express progress
    ############################### 
    # source type info and metadata
    ###############################
    #create type info and metadata for the source
    if copy.source.__class__.__name__ == "CSVConf":
        if copy.source.delimiter == '\\t':
           copy.source.delimiter = '\t'
        src_md = metadata.CSVMetadata(copy.source)
    elif copy.source.__class__.__name__ == "SQLTableConf":
        #unnecessary at the moment--> src_ti = metadata.SQLTypeInfo(copy.source)
        src_md = metadata.SQLTableMetadata(copy.source)
    elif copy.source.__class__.__name__ == "SQLQueryConf":
        src_md = metadata.SQLQueryMetadata(copy.source)
    
    #if copy.optional['debug']:
    #    print("source metadata: {}".format(src_md.__class__.__name__))
    
    ############################### 
    # target type info and metadata
    ###############################
    if copy.target.__class__.__name__ == "CSVConf":
        # create file name
        f = open(copy.target.path, "w", encoding=copy.target.encoding)
        if copy.target.has_header:
            wr = csv.writer(f,delimiter=copy.target.delimiter, lineterminator = "\n")
            wr.writerow([x.column_name for x in src_md.column_list])
        wr = csv.writer(f,delimiter=copy.target.delimiter,quoting=csv.QUOTE_ALL, lineterminator = "\n")

    elif copy.target.__class__.__name__ == "SQLTableConf":
        trg_ti = metadata.SQLTypeInfo(copy.target)
        #### check if target table exists, if not just create one assumming user wants a dump copy eg. without column mappings
        _is_trg_tbl_existence = metadata.is_sql_table_existence(copy.target)
        if _is_trg_tbl_existence:
            copy.target.table_existence = True
        else:
            copy.target.table_existence = False
            if copy.optional['debug']:
                print("table {}.{} does not exist!".format(copy.target.schema_name,copy.target.table_name))
            thrd = threading.Thread(target=metadata.create_simple_sql_table,args=(copy.target,trg_ti,src_md,copy.optional))
            thrd.start()
            while thrd.is_alive():
                if copy.optional['debug']:
                    #print("|", end='', flush=True)
                    sys.stdout.write(next(spinner))   # write the next character             
                    sys.stdout.flush()                # flush stdout buffer (actual character display)
                    sys.stdout.write('\b')            # erase the last written char
                time.sleep(1)
                
            #synchronous call --> metadata.create_simple_sql_table(copy.target,trg_ti,src_md,copy.optional)
            thrd.join()
            
        trg_md = metadata.SQLTableMetadata(copy.target)
    #if copy.optional['debug']:
    #    print("target metadata: {}".format(trg_md.__class__.__name__))
    
    
    ############################### 
    # SQLRecord instance
    ###############################
    if copy.target.type == "sql_table":
        sr = sql_rec.SQLRecord(trg_ti,src_md,trg_md,copy)

        ####validate column matching
        if len(sr.unmatched_column_name_list) > 0:
            print("cannot find matching column(s)  in target table {}.{}: {}".format(",".join(sr.unmatched_column_name_list),trg_md.schema_name,trg_md.table_name))
            quit()
        if len(sr.mapped_column_name_list) == 0:
            print("souce column name: {}".format([x.column_name for x in src_md.column_list]))
            print("target column name: {}".format([x.column_name for x in src_md.column_list]))
            print("Error. column name not fully matching!")
            quit()

    if copy.target.type == "sql_table":
        #sr.record_timestamp = record_timestamp
        ##test
        #sr.sql_stmt_type = "prepared"
        max_row_per_batch = 2  #define max row per batch
        #### system restriction batch max row = 1000, prepared max param = 2100 
        if max_row_per_batch is None:
            max_row_per_batch = int(0.3*2000/len(sr.mapped_column_name_list))
            if max_row_per_batch < 1:
                max_row_per_batch = 1
        #print("max_row_per_batch {}".format(max_row_per_batch))
        rl = rec_load.RecordLoader(copy.target,trg_md,sr,copy.optional["insert_method"],max_row_per_batch)

    #################################################################### 
    # data source iteration, sql record generation and target processing
    ####################################################################
    row_count = 0

    if copy.optional['debug']:
        print("copying data ",end="",flush=True)
    start_time = time.time()
    for row_count,line in enumerate(rec_gen.record_generator(src_md),1):
        #print(line)
        if copy.target.type == "sql_table":
            rl.add_record(sr.gen_sql_record(line))      
            #rl.add_record(_record)
        elif copy.target.type == "csv":
            wr.writerow(line)

        if copy.optional['debug']:
            if row_count % 10000 == 0:
            #if time.time() - start_time > 2:
                #misc.progress()
                sys.stdout.write(str(row_count))
                sys.stdout.flush()
                sys.stdout.write('\b'*len(str(row_count)))
                #start_time = time.time()
    if copy.target.type == "sql_table":
        rl.add_record(False)#signal the record loader to finish up
    if copy.optional['debug']:
        sys.stdout.write(str(row_count))
        sys.stdout.flush()
        sys.stdout.write('\b'*len(str(row_count)))
    
    
    if copy.optional['debug']:
        print("copy id {} completed. {} row(s) affected for {}".format(copy.id,row_count,datetime.datetime.now()-start))


def drop_table(conn_str,schema_name,table_name):
    # check if table exists
    conn = pyodbc.connect(str(conn_str))
    cursor = conn.cursor()
    if cursor.tables(schema=schema_name,table=table_name,tableType="TABLE").fetchone():
        conn = pyodbc.connect(str(conn_str)) 
        cursor = conn.cursor()
        _sql = "DROP TABLE {}.{}".format(schema_name,table_name)
        cursor.execute(_sql)
        conn.commit()
    else:
        # give a warning
        print("Table {}.{} does not exist. function {} was ignored".format(schema_name,table_name,sys._getframe().f_code.co_name))