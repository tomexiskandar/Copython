"""
copython is an engine to copy data to and from sql database.
it defines functions to be called by client codes.
the arg is an xml config file. this file will be used to 
create/complete metadata for the source and target.
"""
import xml.etree.ElementTree as ET
import xml.dom.minidom
import codecs
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

        #cc = copyconf.CopyConf.config_from_xml(config)
        cc = copyconf.CopyConf(config)
        #for c in cc.copy_list:
        #    for k,v in c.__dict__.items():
        #        print(k,v)
        
        if debug:
            cc.debug()
        #cc.validate() must make two validation. one validation about the correct config (complete) and validation about source path/table exists
        
        ##### validate config #####
        # does the source file/sql table exist and complete? if not quit

        # does the source can be accessed or connected? if not quit
        cc.validate()
        
    
    
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
        max_row_per_batch = None  #define max row per batch
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
            #write line to file
            #with open(..., 'wb') as myfile:
            #wr = csv.writer(f, quoting=csv.QUOTE_ALL)
            
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

def gen_xml_cf_template(output_path,src_obj,trg_obj,colmap_src):
    """
    create a template config file as xml file.
    if the trg_obj a sql_table and colmap is requested then it
    requires to connect to the dbms
    """

    source_type = src_obj.type
    target_type = trg_obj.type

    #write the xml file
    root = ET.Element('config')
    child_desc = ET.SubElement(root,"description")
    child_desc.text = "description"
    child_type = ET.SubElement(root,"type")
    child_type.set("source_type",src_obj.type)
    child_type.set("target_type",trg_obj.type)


    #### add copy
    child_copy = ET.SubElement(root,"copy")
    #create id
    id = "unk"
    if src_obj.type == "csv":
        id = os.path.splitext(os.path.basename(src_obj.path))[0]
    elif src_obj.type == "sql_table":
        id = src_obj.table_name
    child_copy.set ("id",id)
    ## add copy's child
    #add source
    copy_child_source = ET.SubElement(child_copy,"source")
    copy_child_source.set("source_type",source_type)
    #copy_child_source_quotechar.text = chr(34)
    for k,v in src_obj.__dict__.items():
        if v is not None:
            copy_child_source.set(k,v)
    #add target
    copy_child_target = ET.SubElement(child_copy,"target")
    for k,v in trg_obj.__dict__.items():
        if v is not None:
            copy_child_target.set(k,v)
    
    
    #add column mapping
    source_column_name_list = []
    target_column_name_list = []
    if colmap_src == "source":
        #get colmap from source as a template
        if(src_obj.type=="csv"):#read the csv header
            with open(src_obj.path, 'r', encoding=src_obj.encoding) as csvfile:
                reader = csv.reader(csvfile,delimiter=src_obj.delimiter,quotechar=src_obj.quotechar)
                if src_obj.has_header:
                    #### add column_name to metadata
                    source_column_name_list = next(reader)
        if(src_obj.type=="sql_table"):#read the table's columns
            conn = pyodbc.connect(str(src_obj.conn_str))
            conn.timeout = 15
            cursor = conn.cursor()
            _col_tuple = cursor.columns(table=src_obj.table_name,schema=src_obj.schema_name).fetchall()
            source_column_name_list = [x.column_name for x in _col_tuple]
        if(src_obj.type=="sql_query"):#read the table's columns
            conn = pyodbc.connect(str(src_obj.conn_str))
            conn.timeout = 15
            cursor = conn.cursor()
            row = cursor.execute(src_obj.sql_str).fetchone()#fetch one just for a dummy executing as we need columns
            _desc_tuple = cursor.description
            for col in _desc_tuple:
                print(col)
            source_column_name_list = [x[0] for x in _desc_tuple]
      
    if colmap_src == "target":
        #get colmap from source as a template
        if(trg_obj.type=="sql_table"):#read the table's columns
            conn = pyodbc.connect(str(trg_obj.conn_str))
            conn.timeout = 15
            cursor = conn.cursor()
            _col_tuple = cursor.columns(table=trg_obj.table_name,schema=trg_obj.schema_name).fetchall()
            target_column_name_list = [x.column_name for x in _col_tuple]
    
    column_name_list_for_colmap = []
    if colmap_src == "source":
        column_name_list_for_colmap = source_column_name_list
    if colmap_src == "target":
        column_name_list_for_colmap = target_column_name_list
    for col in column_name_list_for_colmap:
        child_cm = ET.SubElement(child_copy,"column_mapping")
        child_cm.set("source",col)
        child_cm.set("target",col)
    #add comment for source column name
    if len(source_column_name_list) > 0:
        comment = ET.Comment("source columns: " + ",".join(source_column_name_list))
        child_copy.insert(2+len(source_column_name_list)+ 1,comment)
    if len(target_column_name_list) > 0:
        comment = ET.Comment("target columns: " + ",".join(target_column_name_list))
        child_copy.insert(2+len(target_column_name_list)+ 1,comment)

    #create an xml string
    xml_str = ET.tostring(root)

    xml_str_parsed = xml.dom.minidom.parseString(xml_str)
   
    pretty_xml_as_string = xml_str_parsed.toprettyxml("  ")
    #print(pretty_xml_as_string)

    with codecs.open(output_path, "w", encoding="utf-8") as xml_file:
        xml_file.write(pretty_xml_as_string)
        print("xml config file {} created.".format(output_path))

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