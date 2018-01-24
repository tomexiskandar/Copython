import xml.etree.ElementTree as ET
import xml.dom.minidom
import re
import os.path


class CSVConf():
    def __init__(self):
        self.path = None
        self.encoding = None
        self.delimiter = None
        self.quotechar = None
        self.has_header = None

class SQLTableConf:
    def __init__ (self):
        self.table_name = None
        self.schema_name = None
        self.conn_str = None
        self.table_existence = None

class SQLQueryConf:
    def __init__ (self):
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
        self.source = None
        self.target = None
        self.colmap_list = [] #list of colmap instance
        self.optional = {}

class CopyConf():
    """ the configuration class"""
    def __init__(self):
        # global attributes 
        self.description = None 
        self.source_type = None
        self.target_type = None
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

    def add_copy(self,copy):
        self.copy_list.append(copy)
    def debug(self):
        print("-----start internal config----")
        #print(isinstance(self.copy_list[0].source,SQLTableConf))
        print("description: {}".format(self.description))
        print("source_type: {}".format(self.source_type))
        print("target_type: {}".format(self.target_type))
        for t in self.copy_list:
            print("copy: {}".format(str(t.id)))
            print(" ","source_type: {}".format(t.source_type))
            print(" ","source:")
            #for k,v in getattr(t,"source").__dict__.items():
            for k,v in t.source.__dict__.items():
                print (" "*3,k,v)
            print(" ","target_type: {}".format(t.target_type))
            print(" ","target:")
            for k,v in t.target.__dict__.items():
                print (" "*3,k,v)
            print(" ","colmap_list(source,target):")
            print(" "*3,dict([(x.source,x.target) for x in t.colmap_list]))
            #print("")
        print("-----end internal config----")


    @classmethod
    def config_from_xml(cls,xml_config_path):
        ###### read xml config first then validate its content
        tree = ET.parse(xml_config_path)
        tc_ettemp = tree.getroot()
        tc_et = cls.lowercase_et(cls,tc_ettemp)

        # set global variables
        global_dict = {}
        for el in tc_et.findall("./"):
            #print(el.tag, el.text,el.attrib)
            for k,v in el.attrib.items():
                global_dict[k]=v
            #for ch in el:
            #     print(ch.tag, ch.text,ch.attrib)
        #print(global_dict)
        #print('---------')
        
        # create a config object
        conf = CopyConf()
        
        # get all copies from the config and add them into conf
        copys_et = tc_et.findall("copy")
        
        for copy_et in copys_et:
            c = Copy(copy_et.attrib["id"])
            c.source = cls.get_copy_source(copy_et,global_dict)
            c.target = cls.get_copy_target(copy_et,global_dict)
            c.colmap_list = cls.get_copy_colmap_list(copy_et)
            conf.add_copy(c)
        return conf
    


    def get_copy_source(copy_et,global_dict):
        #collect all the source attributes from config file
        src_dict = {}
        for el in copy_et.findall("source"):
            #get from any attribute element
            for k,v in el.attrib.items():
                src_dict[k] = v            
           
        _src_type = None
        _src_obj = None
        if "source_type" in src_dict:
            _src_type = src_dict["source_type"]
        else:
            #search in global_dict, otherwise raise exception
            if "source_type" in global_dict:
                _src_type = global_dict["source_type"]
            else:
                msg = 'source type for copy id ' + copy_et.attrib["id"] + ' not found'
                raise NameError (msg)
          
        if _src_type.upper() in ["CSV"]:
            _src_obj = CSVConf()
        elif _src_type.upper() in ["SQL_TABLE"]:
            _src_obj = SQLTableConf()
        elif _src_type.upper() in ["SQL_QUERY"]:
            _src_obj = SQLQueryConf()
        
        #validate dict's value:
        if _src_obj.__class__.__name__ == "CSVConf":
            _has_header = src_dict["has_header"]
            if _has_header.upper() in ["YES","Y","TRUE","1"]:
                src_dict["has_header"] = True
            else:
                 src_dict["has_header"] = False
                    
        ##copy the collection into the source object
        for attr in _src_obj.__dict__.keys():
            if attr in src_dict:
                setattr(_src_obj,attr,src_dict[attr])
        return _src_obj

    def get_copy_target(copy_et,global_dict):
        
        #collect all the source attributes from config file
        trg_dict = {}
        for el in copy_et.findall("target"):
            #get from any attribute element
            for k,v in el.attrib.items():
                trg_dict[k] = v            

        _trg_type = None
        _trg_obj = None
        if "target_type" in trg_dict:
            _trg_type = trg_dict["target_type"]
        else:
            #search in global_dict, otherwise raise exception
            if "target_type" in global_dict:
                _trg_type = global_dict["target_type"]
            else:
                msg = 'target type for copy id ' + copy_et.attrib["id"] + ' not found'
                raise NameError (msg)
        
        
        if _trg_type.upper() in ["CSV"]:
            _trg_obj = CSVConf()
        elif _trg_type.upper() in ["SQL_TABLE"]:
            _trg_obj = SQLTableConf()
        elif _trg_type.upper() in ["SQL_QUERY"]:
            _trg_obj = SQLQueryConf()
        
        #validate dict's value:
        if _trg_obj.__class__.__name__ == "CSVConf":
            _has_header = trg_dict["has_header"]
            if _has_header.upper() in ["YES","Y","TRUE","1"]:
                trg_dict["has_header"] = True
            else:
                 trg_dict["has_header"] = False
                    
        ##copy the collection into the source object
        for attr in _trg_obj.__dict__.keys():
            if attr in trg_dict:
                setattr(_trg_obj,attr,trg_dict[attr])
        return _trg_obj

    def get_copy_colmap_list(copy_et):
        _colmap_list = []
        for item in copy_et.findall("column_mapping"):
            for k,v in item.attrib.items():
                if k == "source":
                    _source = v
                if k == "target":
                    _target = v
            _colmap_list.append(ColMapConf(_source,_target))
        return _colmap_list
    
    def lowercase_et(cls,et):
        et.tag = et.tag.lower()  
        for child in et:
            cls.lowercase_et(cls,child)
            #also evaluate the xml attributes
            for k,v in child.attrib.items():
                if k.lower() != k:# if true then swap 
                    child.attrib.pop(k)
                    child.set(k.lower(),v)
        return et

    def validate(self):
        "simple validation to the instance of copyconf"
        # validate source by type
        # if this is a csv then check if the file exists
        if self.source_type.upper() == "CSV":
            for t in self.copy_list:
                if os.path.exists(t.source.path) is False:
                   print("Could not find file {}. Exit...".format(t.source.path))
                   quit()
        # if the colmap is provided then check if columns at the source are matched with the config
        for copy in self.copy_list:
            
            print ([x.source for x in copy.colmap_list])
        quit()