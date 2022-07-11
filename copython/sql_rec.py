#import metadata
#import string
#import multiprocessing
class SQLRecord:
    def __init__(self,trg_ti,source_metadata,target_metadata,copy):
        #self.manager = multiprocessing.Manager()
        #self.tim = type_info_map
        self.src_md = source_metadata
        #self.src_ti = source_type_info
        self.trg_md = target_metadata
        self.trg_ti = trg_ti
        self.copy = copy
        self.insert_method = copy.optional["insert_method"] #create new variable to make it shorter
        self.mapped_target_column_name_list = self.get_mapped_target_column_name_list()
        self.mapped_column_name_list = [x for x in self.mapped_target_column_name_list if x is not None]
        self.unmatched_column_name_list = self.get_unmatched_mapped_column_name_list()

    def gen_sql_record(self,row_value_list):
        # print("")
        # print('row_value',row_value_list)
        # print("")
        # print(self.mapped_column_name_list)
        # quit()
        
        _field_value_list = []
        _param_value_list = []
        #print('mapped columns:',self.mapped_column_name_list)
        for j,v in enumerate(row_value_list): #j is the index starting 0, v is the field value or cell value of the line
            # print("")
            # print(j,v)
            # print("")
            if self.mapped_column_name_list[j] is not None:
                if self.insert_method == "prepared":
                    if v == "":
                        _param_value_list.append(None)
                    else:
                        _param_value_list.append(v)
                        #Error converting data type nvarchar to int _param_value_list.append(self.gen_sql_literal_value(j,v))
                else:# reserved for batch
                    # print('j',j,'v',v)
                    _field_value_list.append(self.gen_sql_literal_value(j,v))
        # print(_field_value_list)
        # print('-----helloooooooooooooooooooo-----')
        
        
        # quit()
        if self.insert_method == "prepared":
            return _param_value_list
        else: # reserved for batch
            return _field_value_list

    #def mp_gen_sql_record(self,row_value_list):
    #    with multiprocessing.Pool(processes=4) as pool:
    #        p = pool.map(self.gen_sql_record,row_value_list)

    def get_mapped_target_column_name_list(self):
        """generate mapped_column_name_list
           if target_table does not exist then copython assumes to create a new table
           and the column names must be from the source metadata.
        """
        #if this is a new table or column mapping not provided then assume user just want to dump the table
        #in a real world dw environment, user must provide column mapping
        if self.copy.target.has_sql_table == False: # or len(self.copy.colmap_list) == 0:
            # print(list([x.column_name for x in self.src_md.column_list]))
            # quit()
            # deprecated return list([x.column_name for x in self.src_md.column_list])
            return self.src_md.bin_table.get_columnnames()


        column_maps = dict([(x.source,x.target) for x in self.copy.colmap_list])

        _mapped_target_column_name_list = []
        # deprecated _src_column_name_list = list([x.column_name for x in self.src_md.column_list])
        _src_column_name_list = self.src_md.bin_table.get_columnnames()
        for source_column in _src_column_name_list:
            if source_column in column_maps:
                for k,v in column_maps.items():
                    if k.lower() == source_column.lower():
                        _mapped_target_column_name_list.append(v)
                        break #if matched then break
            elif source_column not in column_maps:
               _mapped_target_column_name_list.append(None)
        return _mapped_target_column_name_list

    def get_unmatched_mapped_column_name_list(self):
        _unmatched_column_name_list = []
        for _col_name in self.mapped_column_name_list:
            if _col_name not in [x.column_name for x in self.trg_md.column_list]:
                _unmatched_column_name_list.append(_col_name)
        return _unmatched_column_name_list


    def gen_sql_literal_value_old(self,index,value):
        # get column name for this index
        # if empty string then return NULL
        if value == "" or value is None:
            return "NULL"
        # if data_type chars then replace any ' with ''
        col_name = self.mapped_column_name_list[index]
        #print(col_name)
        colidx = next((i for i, item in enumerate(self.trg_md.column_list) if item.column_name == col_name), -1)
        # print(colidx)
        # print('trg colname',self.trg_md.column_list[colidx].column_name)
        # print('trg data type',self.trg_md.column_list[colidx].data_type)
        # print('trg data type',self.trg_md.column_list[index].data_type)
        #quit()
        if self.trg_md.column_list[colidx].data_type in [-10,-9,-8,-1,1,12]:
        #if self.src_md.column_list[_colidx].data_type in [-10,-9,-8,-1,1,12]:
            try:
                value =  value.replace("'","''")
            except:
                pass

        if len(self.trg_ti.type_info_list) > 0:
            _type_name = self.trg_md.column_list[colidx].type_name
            _col_size = self.trg_md.column_list[colidx].column_size
            _lp = self.trg_ti.get_info(_type_name,"literal_prefix")
            _ls = self.trg_ti.get_info(_type_name,"literal_suffix")
            return "{}{}{}".format('' if _lp is None else _lp,value,'' if _ls is None else _ls)
        else:
            return "'{}'".format("".join(value))


    def gen_sql_literal_value(self,index,value): # SLOW
        
        # get column name for this index
        # if empty string then return NULL
        if value == "" or value is None:
            return "NULL"
        # if data_type chars then replace any ' with ''
        col_name = self.mapped_column_name_list[index]
        #print(col_name)
        colidx = next((i for i, item in enumerate(self.trg_md.column_list) if item.column_name == col_name), -1)
        # print(colidx)
        # print('trg colname',self.trg_md.column_list[colidx].column_name)
        # print('trg data type',self.trg_md.column_list[colidx].data_type)
        # print('trg data type',self.trg_md.column_list[index].data_type)
        #quit()
        if self.trg_md.column_list[colidx].data_type in [-10,-9,-8,-1,1,12]:
        #if self.src_md.column_list[_colidx].data_type in [-10,-9,-8,-1,1,12]:
            try:
                value =  value.replace("'","''")
            except:
                pass

        if len(self.trg_ti.type_info_dict) > 0:
            _type_name = self.trg_md.column_list[colidx].type_name
            _col_size = self.trg_md.column_list[colidx].column_size
            #deprecated _lp = self.trg_ti.get_info(_type_name,"literal_prefix")
            _lp = self.trg_ti.type_info_dict[_type_name]['literal_prefix'] # .get_value('literal_prefix','type_name',_type_name) # THIS IS THE CULPRIT FOR SLOW, OVER USE bin_table.iterrows()
            #deprecated _ls = self.trg_ti.get_info(_type_name,"literal_suffix")
            _ls = self.trg_ti.type_info_dict[_type_name]['literal_suffix']  #.get_value('literal_suffix','type_name',_type_name)
            return "{}{}{}".format('' if _lp is None else _lp,value,'' if _ls is None else _ls)
        else:
            return "'{}'".format("".join(value))        
