import xml.etree.ElementTree as ET
import xml.dom.minidom
import json
import csv
import pyodbc
import codecs
import os.path
from copython import metadata

class LODConf():
    def __init__(self):
        self.type = "lod"
        self.lod = None

class FlyTableConf():
    def __init__(self):
        self.type = "flytab"
        self.flytab = None

class CSVConf():
    def __init__(self):
        self.type = "csv"
        self.path = None
        self.encoding = None
        self.delimiter = None
        self.quotechar = None
        self.has_header = None

class SQLTableConf:
    def __init__ (self):
        self.type = "sql_table"
        self.table_name = None
        self.schema_name = None
        self.conn_str = None
        self.table_existence = None # flag assigned in the fly eg. copython to create a simple/tentative table
                                    # at a target end point if the target table has not created yet.

class SQLQueryConf:
    def __init__ (self):
        self.type = "sql_query"
        self.conn_str = None
        self.sql_str = None

class ColMapConf():
    """sub class of copy"""
    def __init__ (self,source,target):
        self.source = source
        self.target = target

class Copy():
    """sub class of config"""
    def __init__(self,id):
        self.id = id
        #### copy end point
        self.source_type = None
        self.target_type = None
        self.source = None       # a copy end point that can be an object of CSVConf or SQLTableConf or SQLQueryConf
        self.target = None       # a copy end point that can be an object of CSVConf or SQLTableConf or SQLQueryConf
        self.colmap_list = []    # list of colmap (column mapping) object that maps a source column to a target column
        self.optional = {}

class CopyConf():
    """ the configuration class"""
    def __init__(self,config=None):
        # global attributes
        self.description = None
        self.source_type = None
        self.target_type = None
        # source as lod
        self.source_lod = None
        # source as csv
        self.source_encoding = None
        self.source_has_header = None
        self.source_delimiter = None
        self.source_quotechar = None
        # source as sql_table
        self.source_conn_str = None
        self.source_schema_name = None
        self.source_table_name = None
        # target as csv
        self.target_encoding = None
        self.target_has_header = None
        self.target_delimiter = None
        self.target_quotechar = None
        # target as sql_table
        self.target_conn_str = None
        self.target_schema_name = None
        self.target_table_name = None
        self.copy_list = []
        self.set_config_attr(config)
    def set_config_attr(self,config):
        if config is not None:
            if config[-4:] == ".xml":
                print("a xml config file passed in...")
                self.set_config_from_xml(config)
            elif config[-5:] == ".json":
                print("a json config file passed in...")
                self.set_config_from_json(config)
        # else:
        #     print("no config file passed in! ({})".format(__class__.__name__))
    def add_copy(self,copy):
        self.copy_list.append(copy)
    def debug(self):
        print("-----start internal config----")
        for k,v in self.__dict__.items():
            if k != 'copy_list':
                print(k,v)
        for c in self.copy_list:
            print("copy: {}".format(str(c.id)))
            print(" ","source_type: {}".format(c.source_type))
            print(" ","source:")
            #for k,v in getattr(c,"source").__dict__.items():
            for k,v in c.source.__dict__.items():
                print (" "*3,k,v)
            print(" ","target_type: {}".format(c.target_type))
            print(" ","target:")
            for k,v in c.target.__dict__.items():
                print (" "*3,k,v)
            print(" ","colmap_list(source,target):")
            print(" "*3,dict([(x.source,x.target) for x in c.colmap_list]))
            #print("")
        print("-----end internal config----")

    def set_config_from_xml(self,xml_config_path):
        ###### read xml config first then validate its content
        tree = ET.parse(xml_config_path)
        tc_ettemp = tree.getroot()
        tc_et = self.lowercase_et(tc_ettemp)

        # set global variables
        global_dict = {}
        for el in tc_et.findall("./"):
            global_dict[el.tag]=el.text
            for k,v in el.attrib.items():
                global_dict[k]=v

        # set global attributes
        for k,v in global_dict.items():
            if k in self.__dict__.keys():
                setattr(self,k,v)

        # get all copies from the config and add them into conf
        copys_et = tc_et.findall("copy")

        for copy_et in copys_et:
            c = Copy(copy_et.attrib["id"])
            ep_dict = self.get_ep_dict_from_xml("source",copy_et)
            c.source = self.get_copy(c,"source",ep_dict,global_dict)
            ep_dict = self.get_ep_dict_from_xml("target",copy_et)
            c.target = self.get_copy(c,"target",ep_dict,global_dict)
            c.colmap_list = self.get_copy_colmap_list_xml(copy_et)
            self.add_copy(c)

    def get_ep_dict_from_xml(self,end_point_name,copy_et):
        #collect all the source attributes from config file
        _ep_dict = {}
        for el in copy_et.findall(end_point_name):
            #get from any attribute element
            for k,v in el.attrib.items():
                _ep_dict[k] = v
        return _ep_dict

    def get_copy(self,copy_obj,end_point_name,ep_dict,global_dict):

        _ep_type = None
        _ep_obj = None
        # evaluate source_type
        searched_key = end_point_name + "_type"
        if searched_key in ep_dict:
            _ep_type = ep_dict[searched_key]
        else:
            #search in global_dict, otherwise raise exception
            if searched_key in global_dict:
                _ep_type = global_dict[searched_key]
            else:
                msg = searched_key + ' for copy id ' + copy_obj.id + ' not found'
                raise NameError (msg)

        # create end point object
        if _ep_type.upper() in ["CSV"]:
            ep_dict = self.get_csv_attr_dict(end_point_name,ep_dict,global_dict)
            #create a csv object
            _ep_obj = CSVConf()
        elif _ep_type.upper() in ["SQL_TABLE"]:
            ep_dict = self.get_sql_table_attr_dict(end_point_name,ep_dict,global_dict)
            _ep_obj = SQLTableConf()
        elif _ep_type.upper() in ["SQL_QUERY"]:
            ep_dict = self.get_sql_table_attr_dict(end_point_name,ep_dict,global_dict)
            _ep_obj = SQLQueryConf()

        ##copy the collection into the source object
        for attr in _ep_obj.__dict__.keys():
            if attr in ep_dict:
                setattr(_ep_obj,attr,ep_dict[attr])
        return _ep_obj

    def get_csv_attr_dict(self,end_point_name,ep_dict,global_dict):
        """
        this function must resolve attribute error (not provided in copy level)
        by searching and assigning the value from global area of config
        """
        # evaluate has_header
        if "has_header" not in ep_dict:
            #search in global_dict, otherwise raise exception
            ep_has_header = end_point_name + "_has_header"
            if ep_has_header in global_dict:
                ep_dict["has_header"] = global_dict[ep_has_header]
            else:
                msg = 'has header for copy id ' + copy_et.attrib["id"] + ' not found'
                raise NameError (msg)
        ## validate has_header
        #_has_header = ep_dict["has_header"]
        #if _has_header.upper() in ["YES","Y","TRUE","1"]:
        #    ep_dict["has_header"] = True
        #else:
        #    ep_dict["has_header"] = False
        # evaluate encoding
        if "encoding" not in ep_dict:
            #search in global_dict, otherwise raise exception
            ep_encoding = end_point_name + "_encoding"
            if ep_encoding in global_dict:
                ep_dict["encoding"] = global_dict[ep_encoding]
            else:
                msg = 'encoding for copy id ' + copy_et.attrib["id"] + ' not found'
                raise NameError (msg)
        # evaluate delimiter
        if "delimiter" not in ep_dict:
            #search in global_dict, otherwise raise exception
            ep_delimiter = end_point_name + "_delimiter"
            if ep_delimiter in global_dict:
                ep_dict["delimiter"] = global_dict[ep_delimiter]
            else:
                msg = 'delimiter for copy id ' + copy_et.attrib["id"] + ' not found'
                raise NameError (msg)
        # evaluate quotechar
        if "quotechar" not in ep_dict:
            #search in global_dict, otherwise raise exception
            ep_quotechar = end_point_name + "_quotechar"
            if ep_quotechar in global_dict:
                ep_dict["quotechar"] = global_dict[ep_quotechar]
            else:
                pass
                #msg = 'quotechar for copy id ' + copy_et.attrib["id"] + ' not found'
                #raise NameError (msg)
        return ep_dict

    def get_sql_table_attr_dict(self,end_point_name,ep_dict,global_dict):
        """
        this function must resolve attribute error (not provided in copy level)
        by searching and assigning the value from global area of config
        """
        # evaluate conn_str
        if "conn_str" not in ep_dict:
            #search in global_dict, otherwise raise exception
            ep_conn_str = end_point_name + "_conn_str"
            if ep_conn_str in global_dict:
                ep_dict["conn_str"] = global_dict[ep_conn_str]
            else:
                msg = 'connection string for copy id ' + copy_et.attrib["id"] + ' not found'
                raise NameError (msg)
        return ep_dict

    def get_sql_query_attr_dict(self,end_point_name,ep_dict,global_dict):
        """
        this function must resolve attribute error (not provided in copy level)
        by searching and assigning the value from global area of config
        """
        # evaluate conn_str
        if "conn_str" not in ep_dict:
            #search in global_dict, otherwise raise exception
            ep_conn_str = end_point_name + "_conn_str"
            if ep_conn_str in global_dict:
                ep_dict["conn_str"] = global_dict[ep_conn_str]
            else:
                msg = 'connection string for copy id ' + copy_et.attrib["id"] + ' not found'
                raise NameError (msg)
        return ep_dict

    def get_copy_colmap_list_xml(self,copy_et):
        _colmap_list = []
        for item in copy_et.findall("column_mapping"):
            for k,v in item.attrib.items():
                if k == "source":
                    _source = v
                if k == "target":
                    _target = v
            _colmap_list.append(ColMapConf(_source,_target))
        return _colmap_list

    def lowercase_et(self,et):
        et.tag = et.tag.lower()
        for child in et:
            self.lowercase_et(child)
            #also evaluate the xml attributes
            for k,v in child.attrib.items():
                if k.lower() != k:# if true then swap
                    child.attrib.pop(k)
                    child.set(k.lower(),v)
        return et

    def validate_config_object(self):
        for c in self.copy_list:
            if c.source.type == "csv":
                # validate has_header
                if type(c.source.has_header) is not bool:
                    _has_header = c.source.has_header
                    if _has_header.upper() in ["YES","Y","TRUE","1"]:
                        c.source.has_header = True
                    else:
                        c.source.has_header = False

    def validate(self):
        """ UNDER DEVELOPMENT
        simple validation to the instance of copyconf"""

        # validate source by type
        # if this is a csv then check if the file exists
        for c in self.copy_list:
            if c.source.type == "csv":
                if os.path.exists(c.source.path) is False:
                    print("Error 1. Could not find file {}. Exiting...".format(c.source.path))
                    quit()
            if c.source.type == "sql_table":
                _is_tbl_existence = metadata.is_sql_table_existence(c.source)
                if _is_tbl_existence is False:
                    print("Error 2. Could not find table {}.{}. Exiting...".format(c.source.schema_name,c.source.table_name))
                    quit()
                


        # if the colmap is provided then check if columns at the source are matched with the config
        #for copy in self.copy_list:

        #    print ([x.source for x in copy.colmap_list])
        #quit()

    def set_config_from_json(self,config):
        ###### read json config first then validate its content
        cc_json = json.load(open(config))

        # set global variables
        global_dict = {}
        for k,v in cc_json.items():
            if type(v) == str:
                global_dict[k]=v
            if type(v)==dict:
                # go to its first child
                for k,v in v.items():
                    if type(v) == str:
                        global_dict[k]=v

        # set global attributes
        for k,v in global_dict.items():
            if k in self.__dict__.keys():
                setattr(self,k,v)

        # get all copies from the config and add them into conf
        copies = cc_json["copy"]

        for copy in copies:
            c = Copy(copy["id"])
            #ep_dict = self.get_ep_dict_from_xml("source",copy)
            ep_dict = copy["source"]
            c.source = self.get_copy(c,"source",ep_dict,global_dict)
            ep_dict = copy["target"]
            c.target = self.get_copy(c,"target",ep_dict,global_dict)
            c.colmap_list = self.get_copy_colmap_list_json(copy)
            self.add_copy(c)

    def get_copy_colmap_list_json(self,copy):
        _colmap_list = []
        for cm in copy["column_mapping"]:
            for k,v in cm.items():
                if k == "source":
                    _source = v
                if k == "target":
                    _target = v
            _colmap_list.append(ColMapConf(_source,_target))
        return _colmap_list


"""
tool for generate config file
"""
def gen_cft(output_path   # the location of the output
            ,file_format  # xml or json
            ,src_obj      # source instance
            ,trg_obj      # target instance
            ,colmap_src   # column mapping source
            ):
    # validate output path
    if file_format == "json" and output_path[-5:] != ".json":
        #correct the extension
        output_path = output_path + ".json"
    if file_format == "xml" and output_path[-4:] != ".xml":
        #correct the extension
        output_path = output_path + ".xml"
    if output_path[-5:] != ".json" and output_path[-4:] != ".xml" and file_format.lower() in ["json","xml"]:
        output_path = output_path + "." + file_format
    if file_format == "json":
        gen_json_cf_template(output_path,src_obj,trg_obj,colmap_src)
    elif file_format == "xml":
        gen_xml_cf_template(output_path,src_obj,trg_obj,colmap_src)

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

    # get valid attr for the source
    valid_src_attr_dict = get_valid_attr_dict(src_obj)
    for k,v in valid_src_attr_dict.items():
        if v is not None:
            copy_child_source.set(k,v)
    #add target
    copy_child_target = ET.SubElement(child_copy,"target")
    # get valid attr for the target
    valid_trg_attr_dict = get_valid_attr_dict(trg_obj)
    for k,v in valid_trg_attr_dict.items():
        if v is not None:
            copy_child_target.set(k,v)


    #add column mapping
    source_column_name_list = []
    target_column_name_list = []
    #if colmap_src == "source":
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

    #if colmap_src == "target":
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

    with codecs.open(output_path, "w", encoding="utf-8") as xml_file:
        xml_file.write(pretty_xml_as_string)
        print("xml config file {} created.".format(output_path))

def gen_json_cf_template(output_path,src_obj,trg_obj,colmap_src):
    """
    create a template config file as xml file.
    if the trg_obj a sql_table and colmap is requested then it
    requires to connect to the dbms
    """

    source_type = src_obj.type
    target_type = trg_obj.type

    #create a dictionary for copyconf
    cc_dict = {}
    # add description
    cc_dict["description"] = "description"
    # add global type
    global_type_dict = {"source_type":source_type,"target_type":target_type}
    cc_dict["type"] = global_type_dict

    # add copy list. I'd better add this as array instead of object
    # thus easy to add more copy if eventually the user need it
    copy_list = []
    # add a copy dict (id,source,target,colmap) from the funct args.
    copy = {}
    # add id
    id = "unk"
    if src_obj.type == "csv":
        id = os.path.splitext(os.path.basename(src_obj.path))[0]
    elif src_obj.type == "sql_table":
        id = src_obj.table_name
    copy["id"] = id
    # gen source dictionary
    valid_src_attr_dict = get_valid_attr_dict(src_obj)
    copy["source"] = valid_src_attr_dict
    # gen target dictionary
    valid_trg_attr_dict = get_valid_attr_dict(trg_obj)
    copy["target"] = valid_trg_attr_dict

    # add copy to the copy list
    copy_list.append(copy)
    cc_dict["copy"]=copy_list

    #add column mapping
    source_column_name_list = []
    target_column_name_list = []
    #if colmap_src == "source":
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

    #if colmap_src == "target":
        #get colmap from target as a template
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
    # gen colmap_list
    colmap_list = []
    for col in column_name_list_for_colmap:
        #child_cm = ET.SubElement(child_copy,"column_mapping")
        #child_cm.set("source",col)
        #child_cm.set("target",col)
        colmap = {}
        colmap["source"] = col
        colmap["target"] = col
        colmap_list.append(colmap)
    # add colmap list to copy
    copy["column_mapping"] = colmap_list
    #add comment for source column name
    if len(source_column_name_list) > 0:
        #comment = ET.Comment("source columns: " + ",".join(source_column_name_list))
        #child_copy.insert(2+len(source_column_name_list)+ 1,comment)
        copy["_comment_src_cols"] = ",".join(source_column_name_list)
    if len(target_column_name_list) > 0:
        #comment = ET.Comment("target columns: " + ",".join(target_column_name_list))
        #child_copy.insert(2+len(target_column_name_list)+ 1,comment
        copy["_comment_trg_cols"] = ",".join(target_column_name_list)

    # write cc_dict as json file
    with open(output_path,"w",encoding="utf-8") as f:
        json.dump(cc_dict,f,indent=4,sort_keys=False)
    print("json config file {} created.".format(output_path))

def get_valid_attr_dict(ep_obj):
    """ eliminate system attributes from being presented in config file"""
    #current system attribute list (this may grow)
    system_attr = ["table_existence"]
    _valid_attr_dict = {}
    for k,v in ep_obj.__dict__.items():
        if k not in system_attr:
            _valid_attr_dict[k] = v
    return _valid_attr_dict
