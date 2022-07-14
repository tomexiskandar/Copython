#import metadata
#import string
#import multiprocessing
class SQLRecord:
    def __init__(self,trg_ti,source_metadata,target_metadata,copy):
        self.src_md = source_metadata
        self.trg_md = target_metadata
        self.trg_ti = trg_ti
        self.copy = copy
        self.insert_method = copy.optional["insert_method"] #create new variable to make it shorter
        self.mapped_target_columnnames_list = [x.target for x in copy.colmap_list] 
        self.unmatched_column_name_list = self.get_unmatched_mapped_column_name_list()

    def gen_sql_record(self,rowdict):
        _field_value_list = []
        _param_value_list = []
        for k, v in rowdict.items(): #j is the index starting 0, v is the field value or cell value of the line
            if k in self.mapped_target_columnnames_list:
                if self.insert_method == "prepared":
                    if v == "":
                        _param_value_list.append(None)
                    else:
                        _param_value_list.append(v)
                else:# reserved for batch
                    _field_value_list.append(self.gen_sql_literal_value(k,v))
        if self.insert_method == "prepared":
            return _param_value_list
        else: # reserved for batch
            return _field_value_list
        return _mapped_target_column_name_list


    def get_unmatched_mapped_column_name_list(self):
        _unmatched_column_name_list = []
        for _col_name in self.mapped_target_columnnames_list:
            if _col_name not in [x.column_name for x in self.trg_md.column_list]:
                _unmatched_column_name_list.append(_col_name)
        return _unmatched_column_name_list


    def gen_sql_literal_value(self,columnname,value):
        # get column name for this index
        # if empty string then return NULL
        if value == "" or value is None:
            return "NULL"
        colidx = next((i for i, item in enumerate(self.trg_md.column_list) if item.column_name == columnname), -1)
        if self.trg_md.column_list[colidx].data_type in [-10,-9,-8,-1,1,12]:
            try:
                value =  value.replace("'","''")
            except:
                pass

        if len(self.trg_ti.type_info_dict) > 0:
            _type_name = self.trg_md.column_list[colidx].type_name
            _col_size = self.trg_md.column_list[colidx].column_size
            _lp = self.trg_ti.type_info_dict[_type_name]['literal_prefix']
            _ls = self.trg_ti.type_info_dict[_type_name]['literal_suffix']
            return "{}{}{}".format('' if _lp is None else _lp,value,'' if _ls is None else _ls)
        else:
            return "'{}'".format("".join(value))