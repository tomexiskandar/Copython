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
import logging

log = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)10s() ] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel(logging.ERROR)


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

def copy_data(config, debug=False, multi_process=False):
    if debug == True:
        log.setLevel(logging.DEBUG)

    # try:
        #print(pyodbc.drivers())
        #quit()

        # determine CopyConf instance depending user input (whether a CopyConf instance or path of xml/json config file)
        if type(config)== str:
            # user pass a path of config file
            cc = copyconf.CopyConf(config)
        if config.__class__.__name__ == 'CopyConf':
            # user pass an instance of CopyConf
            cc = config

        #cc = copyconf.CopyConf(config)
        #for c in cc.copy_list:
        #    for k,v in c.__dict__.items():
        #        print(k,v)

        # validate config
        cc.validate_config_object()

        # validate config for existence/accessible
        cc.validate()

        if debug:
            cc.debug()

        # multi_process = True
        if multi_process is True:
            copies = []
            copies =  [c for c in cc.copy_list]

            for copy in copies:
                if copy.optional['insert_method'] is None:
                    copy.optional['insert_method'] = 'prepared'

            with Pool(processes=4) as pool:
                p = pool.map(execute_copy,copies)
        else:
            for copy in cc.copy_list:
                if copy.optional['insert_method'] is None:
                    copy.optional['insert_method'] = 'prepared'
 
                execute_copy(copy, debug)
        return 0
    # except Exception as e:
    #     return e


def execute_copy(copy, debug):
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
        src_md = metadata.SQLTableMetadata(copy.source)
    elif copy.source.__class__.__name__ == "SQLQueryConf":
        src_md = metadata.SQLQueryMetadata(copy.source)
    elif copy.source.__class__.__name__ == "LODConf":
        src_md = metadata.LODMetadata(copy.source)
    elif copy.source.__class__.__name__ == "BinTableConf":
        src_md = metadata.BinTableMetadata(copy.source)

    
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
        
        trg_ti = metadata.SQLTypeInfo2(copy.target)
        
        #### check if target table exists, if not just create one assumming user wants a dump copy eg. without column mappings
        if metadata.has_sql_table(copy.target):
            copy.target.has_sql_table = True
            if len(copy.colmap_list) == 0:
                for columnname in src_md.column_name_list:
                    colmap = copyconf.ColMapConf(columnname,columnname)
                    copy.colmap_list.append(colmap)
        else:
            copy.target.has_sql_table = False
            log.debug("table {}.{} does not exist! Colmap will be ignored!".format(copy.target.schema_name,copy.target.table_name))
            create_table_with_new_thread = False # may involve table scanning for data properties, so it'll take longer
            # create colmap if colmap not provided
            if len(copy.colmap_list) == 0:
                for columnname in src_md.column_name_list:
                    colmap = copyconf.ColMapConf(columnname,columnname)
                    copy.colmap_list.append(colmap)
            if create_table_with_new_thread == True:    
                thrd = threading.Thread(target=metadata.create_simple_sql_table,args=(copy.target,trg_ti,src_md,copy.optional))
                thrd.start()
                while thrd.is_alive():
                    if debug: #copy.optional['debug']:
                        #print("|", end='', flush=True)
                        sys.stdout.write(next(spinner))   # write the next character
                        sys.stdout.flush()                # flush stdout buffer (actual character display)
                        sys.stdout.write('\b')            # erase the last written char
                    time.sleep(1)
                thrd.join()
            else:
                #synchronous call 
                metadata.create_simple_sql_table(copy.target,trg_ti,src_md,copy.optional)    

        trg_md = metadata.SQLTableMetadata(copy.target)
    
    
    ###############################
    # validate colmap and output some warnings
    ###############################
    mapped_target_columnnames_list = [x.target for x in copy.colmap_list]
    errors = []
    if copy.target.has_sql_table:
        # validate source columns
        src_col_names = src_md.bin_table.get_columnnames()
        for src_colmap in [x.source for x in copy.colmap_list]:
            if src_colmap not in src_col_names:
                errors.append('source column {} is not found in its datasource'.format(src_colmap))
                log.error('Error! source column {} is not found in its datasource'.format(src_colmap))
        #validate target columns
        trg_col_names = [x.column_name for x in trg_md.column_list]
        for trg_colmap in [x.target for x in copy.colmap_list]:
            if trg_colmap not in trg_col_names:
                errors.append('target column {} is not found in its datasource'.format(trg_colmap))
                log.error('Error! target column {} is not found in its datasource'.format(trg_colmap))
               
    if len(errors) > 0:
        log.error('There are errors! Program is terminated...')
        quit()

    ###############################
    # SQLRecord instance
    ###############################
    if copy.target.type == "sql_table":
        sr = sql_rec.SQLRecord(trg_ti,src_md,trg_md,copy)
        ####validate column matching
        if len(sr.unmatched_column_name_list) > 0:
            log.debug("error! cannot find matching column(s)  in target table {}.{}: {}".format(trg_md.schema_name,trg_md.table_name,",".join(sr.unmatched_column_name_list)))
            log.debug("exiting....")
            quit()
        
    if copy.target.type == "sql_table":
        max_row_per_batch = 1 #2  #define max row per batch
        if copy.optional["insert_method"] == 'batch':
            #### system restriction batch max row = 1000, prepared max param = 2100
            max_row_per_batch = int(0.3*2000/len(mapped_target_columnnames_list))
            if max_row_per_batch < 1:
                max_row_per_batch = 1
        rl = rec_load.RecordLoader(copy.target,trg_md,mapped_target_columnnames_list,copy.optional["insert_method"],max_row_per_batch)

    ####################################################################
    # data source iteration, sql record generation and target processing
    ####################################################################

    row_count = 0

    if debug:
        print("copying data ",end="",flush=True)
    start_time = time.time()
    
    for row_count,rowdict in enumerate(rec_gen.record_generator(src_md, copy),1):
        if copy.target.type == "sql_table":
            rl.add_record(sr.gen_sql_record(rowdict))
        elif copy.target.type == "csv":
            wr.writerow([x for x in rowdict.values()])

        if debug:
            if row_count % 10000 == 0:
                sys.stdout.write(str(row_count))
                sys.stdout.flush()
                sys.stdout.write('\b'*len(str(row_count)))
    if copy.target.type == "sql_table":
        rl.add_record(False)#signal the record loader to finish up
    if debug:
        sys.stdout.write(str(row_count))
        sys.stdout.flush()
        sys.stdout.write('\b'*len(str(row_count)))
        print('\n')
        pass

    log.debug("copy {} completed. {} row(s) affected for {}".format(copy.id,row_count,datetime.datetime.now()-start))


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
        log.warning("Table {}.{} does not exist. function {} was ignored".format(schema_name,table_name,sys._getframe().f_code.co_name))
