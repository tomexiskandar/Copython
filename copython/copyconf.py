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
        self.source = None
        self.target = None
        self.colmap_list = [] #list of colmap instance
        self.optional = {}

class CopyConf():
    """ the configuration class"""
    def __init__(self,description,source_type,target_type):
        self.description = description
        self.source_type = source_type
        self.target_type = target_type
        #self.sql_stmt_type = None
        self.copy_list = []

    def add_copy(self,copy):
        self.copy_list.append(copy)
    def debug(self):
        print("-----config----")
        #print(isinstance(self.copy_list[0].source,SQLTableConf))
        print("description: {}".format(self.description))
        print("source_type: {}".format(self.source_type))
        print("target_type: {}".format(self.target_type))
        for t in self.copy_list:
            print("copy: {}".format(str(t.id)))
            print(" ","source:")
            #for k,v in getattr(t,"source").__dict__.items():
            for k,v in t.source.__dict__.items():
                print (" "*3,k,v)
            print(" ","target:")
            for k,v in t.target.__dict__.items():
                print (" "*3,k,v)
            print(" ","colmap_list(source,target):")
            print(" "*3,dict([(x.source,x.target) for x in t.colmap_list]))
            print("")
    


    @classmethod
    def config_from_xml(cls,xml_config_path):
        tree = ET.parse(xml_config_path)
        tc_ettemp = tree.getroot()
        tc_et = cls.lowercase_et(cls,tc_ettemp)
        #for el in tc_et:
        #    print(el.tag, el.text)
        #quit()

        desc = tc_et.find("description").text
        src_type = tc_et.find("source_type").text
        trg_type = tc_et.find("target_type").text
    
        #### create a config class cls(desc,src_type,trg_type)#
        conf = CopyConf(desc,src_type,trg_type)
        
        # get all copys from the config and add them into conf
        copys_et = tc_et.findall("copy")
    
        for copy_et in copys_et:
        
            c = Copy(copy_et.attrib["id"])
            c.source = cls.get_copy_source(copy_et,src_type)
            c.target = cls.get_copy_target(copy_et,trg_type)
            c.colmap_list = cls.get_copy_colmap_list(copy_et)
    
            conf.add_copy(c)
        return conf
    


    def get_copy_source(copy_et,source_type):
        if source_type.upper() in ["CSV"]:
            _source = CSVConf()
        elif source_type.upper() in ["SQL_TABLE"]:
            _source = SQLTableConf()
        elif source_type.upper() in ["SQL_QUERY"]:
            _source = SQLQueryConf()
            

        #collect all the source attributes from config file
        src_et_dict = {}
        for item in copy_et.findall("source"):
            #get from any attribute element
            for k,v in item.attrib.items():
                src_et_dict[k] = v            
            #get from child element
            for item in item:
                src_et_dict[item.tag] = item.text
                #get connection string
                if item.tag == "connection_string":
                    cs_attr = item.find("add").attrib
                    src_et_dict["conn_str"] = cs_attr.get("conn_str")
        
        #validate dict's value:
        if _source.__class__.__name__ == "CSVConf":
            _has_header = src_et_dict["has_header"]
            if _has_header.upper() in ["YES","Y","TRUE","1"]:
                src_et_dict["has_header"] = True
            else:
                 src_et_dict["has_header"] = False
        
        ##copy the collection into the source instance
        for attr in _source.__dict__.keys():
            if attr in src_et_dict:
                setattr(_source,attr,src_et_dict[attr])
        return _source

    def get_copy_target(copy_et,target_type):
        
        if target_type.upper() in ["SQL_TABLE"]:
            _target = SQLTableConf()
        elif target_type.upper() in ["CSV"]:
            _target = CSVConf()
        else:
            print("target_type {} is undefined! Exit...".format(target_type))
            quit()
        #collect all the source attributes from config file
        trg_et_dict = {}
        for item in copy_et.findall("target"):
            #get from any attribute element
            for k,v in item.attrib.items():
                trg_et_dict[k] = v
           
            #get from child element
            for item in item:
                trg_et_dict[item.tag] = item.text 
                #print(item.tag,item.text)
                #get connection string
                if item.tag == "connection_string":
                    cs_attr = item.find("add").attrib
                    trg_et_dict["conn_str"] = cs_attr.get("conn_str")
       
        ##copy the collection into the source instance
        for attr in _target.__dict__.keys():
            if attr in trg_et_dict:
                setattr(_target,attr,trg_et_dict[attr])
        return _target

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
        


