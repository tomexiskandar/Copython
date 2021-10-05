import json
import copy

def match_two_paths(jcpath,datapath_list):
    res = False
    #if len(jcpath) == len(datapath_list):
    matches = 0
    tup = list(zip(jcpath,datapath_list))
    #print(" tup tomatch: ",tup)
    for pair in tup:
        #print(' pair:',pair)
        if pair[0] == "#" and isinstance(pair[1],int):
            matches += 1
        else:
            if pair[0] == pair[1]:
                matches += 1
    if len(jcpath) == matches:
        res = True
    return res


class Data_Cell():
    def __init__ (self,data_source,table_name,column_name,value):
        self.data_source = data_source
        self.table_name = table_name
        self.column_name = column_name
        self.data_type = None
        self.column_size = None
        self.decimal_digits = None
        self.ordinal_position = None
        self.value = value
    def __str__(self):
        return "dc(data_source=" + self.data_source +" ,table_name=" + self.table_name+', column_name=' + self.column_name + ', value='+ str(self.value) + ")"
    # def __repr__(self):
    #     return str(self.__str__())

class Data_Row():
    def __init__(self,current_row):
        self.datarow = current_row
    def print_kv(self):
        columns = [str(x.column_name) for x in self.datarow]
        values  = [str(x.value) for x in self.datarow]
        return dict(zip(columns,values))
    def print_all_dc(self):
        column_name_list=[]
        for dc in self.datarow:
            column_name_list.append(dc.column_name)
        print(column_name_list)
    def get_value(self,column_name):
        for dc in self.datarow:
            if dc.column_name == column_name:
                return dc.value

    def add_dc(self,dc):
        self.datarow.append(dc)




class Fly_Table():
    def __init__ (self,name):
        self.name = name
        self.is_primary = False
        self.base_path = None
        self.base_path_list = None
        self.child_table_list = []
        self.jsonpath = []
        self.columns_tocopy = [] # list of columns from other table to be copied
        #
        self.referencing_columns = [] # list of columns that other tables refer to at columns_tocopy
        self.referencing_tables = [] # a list of tables from self.referencing_columns = []

        self.colmap = None
        # too obscured self.parent = None
        # too obsured self.child = []
        self.current_datapath = None
        self.current_slice = None
        self.has_row_completed = False
        self.current_row = []
        self.rows = {} # dictionary of index and row [] #list of Dicionary(lod)
        self.counter = 0
        self.last_ref_updates_rowid = 0


    def gen_pathids_fullpath(self):
        #example of datapath_list 	 ['Results', 0, 'Manufacturers', 1, 'Address', 'AddressLine1']
        #example of base_path 	     ['Results', '#', 'Manufacturers', '#']
        #example of return           [{Result/#=0},{Result/#/Manufacturers/#=1}]
        pathids = []
        for i,item in enumerate(self.base_path_list):
            #print(i,item)
            if item == '#':
                value = self.current_datapath[i]
                phid = []
                for j in range(i+1):
                    #print('j',j)
                    phid.append(self.base_path_list[j])
                #print("phid",phid)
                _pdict = {'/'.join(phid):value}
                #print('_pdict',_pdict)
                pathids.append(_pdict)
        return pathids

    def gen_pathids(self):
        #example of datapath_list 	    ['Results', 0, 'Manufacturers', 1, 'Address', 'AddressLine1']
        #example of base_path 	['Results', '#', 'Manufacturers', '#']
        #example of return          [{Result=0},{Manufacturers=1}]
        pathids = []
        for i,p in enumerate(self.base_path_list):
            if p != "#":
                _pdict = {p:self.current_datapath[i+1]}
                pathids.append(_pdict)
        return pathids

        # pathids = []
        # base_path_length = int(len(self.base_path_list)/2)
        # for i in range (base_path_length):
        #     print(i)
        # print(pathids)
        # quit()
        # return pathids



    def set_referenced_values(self,jc):
        print('--tablename>',self.name,self.referencing_columns)
        # if self.name == 'lic__products':
        #     tbl_idx= next((i for i, item in enumerate(jc.tables) if item.name == "prod__packs"), -1)
        #     print('-->',jc.tables[tbl_idx].current_row)
        #print(self.name,self.referencing_columns)
        pathids = self.gen_pathids_fullpath()

        # print('????????')
        # print(self.referencing_columns)
        # update tab.current_row
        rc_with_values = copy.deepcopy(self.referencing_columns)
        # for dc in self.current_row:
        #     print(dc)
        # quit()
        for rc in rc_with_values:
            for _datacell in self.current_row:
                if rc["referenced_columnname"] == _datacell.column_name:
                    rc["referenced_value"] = _datacell.value
                    # prefix @ for the source of colmap
                    rc["referenced_columnname"] = '@' + _datacell.column_name
                    break

        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print('rc_with_values:',rc_with_values)
        # quit()
        # example of content for rc_with_values:
        # [{'referencing_tablename': 'Manufacturers', 'referenced_tablename': 'artg_entry', 'referenced_columnname': 'LicenceId', 'referenced_value': '92017'}
        #  ,{'referencing_tablename': 'Manufacturers', 'referenced_tablename': 'artg_entry', 'referenced_columnname': 'LicenceClass', 'referenced_value': 'CLAS1'}]

        # distribute to any referencing tables:
        for _rcv in rc_with_values:
            tbl_idx= next((i for i, item in enumerate(jc.tables) if item.name == _rcv["referencing_tablename"]), -1)
            for rowid in range(jc.tables[tbl_idx].last_ref_updates_rowid,len(jc.tables[tbl_idx].rows)):
                #print('----counter--->',self.name,rowid,jc.tables[tbl_idx].last_ref_updates_rowid)
                row = jc.tables[tbl_idx].rows[rowid]
            #for rowid,row in jc.tables[tbl_idx].rows.items():
                # print('row---->',rowid,row.print_kv())
                # check if the row contains the same pathid
                matches = 0
                for p in pathids:
                    #print('--->p',p)
                    for k,v in p.items():
                        for dc in row.datarow:
                            # if this dc is a referenced table
                            if dc.data_source != dc.table_name:
                                # then match the data_source and value
                                # print(dc)
                                if k == dc.data_source and v == dc.value:
                                #    # print('-->match',dc)
                                    matches += 1
                if matches == len(pathids):
                    #print('!!!! matched')
                    dc = Data_Cell(self.name,self.name,_rcv["referenced_columnname"],_rcv["referenced_value"])
                    row.datarow.append(dc)
                    self.counter += 1
                    if self.is_primary:
                        jc.tables[tbl_idx].last_ref_updates_rowid = rowid

                    # tag row that is already process, so the loop is shorter when it repeats


            #update the current_row
            cr_matches = 0
            for p in pathids:
                #print('--->p',p)
                for k,v in p.items():
                    for dc in jc.tables[tbl_idx].current_row:
                        # if this dc is a referenced table
                        if dc.data_source != dc.table_name:

                            if k == dc.data_source and v == dc.value:
                                # then match the data_source and value
                                #print('------match-----',dc)
                                cr_matches += 1
            if cr_matches == len(pathids):
                dc = Data_Cell(self.name,self.name,_rcv["referenced_columnname"],_rcv["referenced_value"])
                jc.tables[tbl_idx].current_row.append(dc)
                self.counter += 1


    def copy_ref_data(self,jt,jc):
        if len(self.columns_tocopy) > 0:
            #for coltc in jt.tables[1].columns_tocopy:
            for coltc in self.columns_tocopy:
                #print(tab.name,'columns tocopy',coltc)
                #loop through target and source table and do the matching
                tbl_idx_s= next((i for i, item in enumerate(jc.tables) if item.name == coltc["tablename"]), -1)
                # print(tbl_idx_s)
                # quit()
                jc.tables[tbl_idx_s].last_ref_updates_rowid = 0
                ########## target table ##########
                #for idx_t, row_t in jt.tables[1].rows.items():
                for idx_t, row_t in self.rows.items():
                    #print(' prod',idx,row.print_kv())
                    ########## source table ##########
                    # for idx_s, row_s in jt.tables[tbl_idx_s].rows.items():
                    for idx_s in range(jc.tables[tbl_idx_s].last_ref_updates_rowid,len(jc.tables[tbl_idx_s].rows)):
                        # match the target and the source based upon provided keys
                        row_s = jc.tables[tbl_idx_s].rows[idx_s]
                        matches = 0
                        for k in coltc["keys"]:
                            # print('key ',k)
                            if row_t.get_value(k) == row_s.get_value(k):
                                matches += 1
                                # print('! matching...')
                        if matches == len(coltc["keys"]):
                            # this is a match! so make a copy
                            # print('!!!! a match')
                            colname_toadd = '@' + coltc["columnname"] #prefix with @ to differentiate with jsonpath
                            dc = Data_Cell(jt.tables[tbl_idx_s].name,self.name,colname_toadd,row_s.get_value(coltc["columnname"]))
                            row_t.add_dc(dc)

                            #flag last row
                            jc.tables[tbl_idx_s].last_ref_updates_rowid = idx_s


class JSONPath_Tables_Settings:
    def __init__(self):
        self.allow_table_value_aslist_dict = False
class JSONPath_Tables_Conf():
    def __init__(self,path):
        self.path = path
        self.jsondata = None
        self.settings = JSONPath_Tables_Settings()
        self.tables = []
        self.intertables_column_copies = []
        self.primary_table = None
        self.get_json_fromfile(self.path)
        self.set_settings()
        self.create_tables()
        self.set_primary_table()
        self.generate_child_tables()

    def generate_child_tables(self):
        for tab in self.tables:
            # print(tab.name,len(tab.base_path))
            # print(' ',tab.base_path)
            # do not evaluate myself
            for othertab in self.tables:
                # do not evaluate myself
                if tab.name != othertab.name:
                    # print('   ',othertab.base_path)
                    # print('   ->',othertab.base_path[0:len(tab.base_path)])
                    if tab.base_path == othertab.base_path[0:len(tab.base_path)]:
                        descendant = othertab.base_path[len(tab.base_path):]
                        descendant_aslist = list(filter(None,descendant.split('/')))
                        #list(filter(None,_value_list))
                        # print('   ->>>',descendant_aslist)
                        # if a child
                        if len(descendant_aslist) == 2:
                            # add to my list
                            tab.child_table_list.append(othertab.name)


    def set_settings(self):
        # for setting,value in self.settings.__dict__.items():
        #     print(setting,value)
        for k,v in self.jsondata["settings"].items():
            if k in [item for item in self.settings.__dict__.keys()]:
                # set the value
                setattr(self.settings,k,v)


    def get_json_fromfile(self,path):
        #print('parsing json file "{}"'.format(path))
        jsondata = json.load(open(path))
        self.jsondata = jsondata

    def create_tables(self):
        #self.tables = [x["name"] for x in self.jsondata["tables"]]
        for tab in self.jsondata["tables"]:
            # print("----tab----")
            # print(tab)
            # create a fly table
            flytab = Fly_Table(tab["name"])
            if 'is_primary' in tab:
                flytab.is_primary = tab['is_primary']

            flytab.base_path = tab["base_path"]
            flytab.base_path_list = tab["base_path"].split('/')

            if 'columns_tocopy' in tab:
                flytab.columns_tocopy = tab['columns_tocopy']

            for jp in tab["colmap"]:
                #jplist = jp.split("/")
                if jp[:1] != '@':
                    flytab.jsonpath.append(jp)

            flytab.colmap = tab["colmap"]

            self.tables.append(flytab)

        # set_intertables_column_copies
        # and allocate back to each table for ref columns and tables
        # the reason we do these allocation at config level instead table level
        # is performance. These information needed when the json data traversing
        # taking place.
        self.set_intertables_column_copies()
        self.allocate_referencing_columns()
        self.allocate_referencing_tables()


    def set_intertables_column_copies(self):
        for tab in self.tables:
            if len(tab.columns_tocopy) > 0:
                #print(tab.name,tab.columns_tocopy)
                for ctc in tab.columns_tocopy:
                    _rel_dict = {}
                    _rel_dict["referencing_tablename"] = tab.name
                    _rel_dict["referenced_tablename"]  = ctc["tablename"]
                    _rel_dict["referenced_columnname"] = ctc["columnname"]
                    self.intertables_column_copies.append(_rel_dict)


    def allocate_referencing_columns(self):
        for tab in self.tables:
            _referencing_columns = []
            for ic in self.intertables_column_copies:
                if tab.name == ic["referenced_tablename"]:
                    tab.referencing_columns.append(ic)


    def allocate_referencing_tables(self):
        # use the list in referencing tables and populate`
        # the tables
        for tab in self.tables:
            _referencing_tables = []
            for rc in tab.referencing_columns:
                #print(' rc:',rc)
                _referencing_tables.append(rc["referencing_tablename"])
            if len(_referencing_tables) > 0:
                _referencing_tables_unique = set(_referencing_tables)
                for _rt in _referencing_tables_unique:
                    tab.referencing_tables.append(_rt)

    def set_primary_table(self):
        _list = []
        for tab in self.tables:
            tup = (len(tab.base_path_list),tab.name)
            _list.append(tup)
        sorted_list = sorted(_list, key=lambda x: x[0])

        # set it
        for tab in self.tables:
            # if table name appears in the first tuple
            if tab.name == sorted_list[0][1]:
                # then assign this table as primary
                tab.is_primary = True


def traverse(dict_or_list, path=[]):
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    else:
        iterator = enumerate(dict_or_list)
    for k, v in iterator:
        yield path + [k], v
        if isinstance(v, (dict, list)):
            for k, v in traverse(v, path + [k]):
                yield k,v
                # yield only non dict or list
                # if not isinstance(v,(dict,list)):
                #     yield k, v

def gen_json_tables(jc,data,data_injection_lod = None):
    for datapath_list, datavalue in traverse(data):
        # check settting whether it allows value as dict/list
        if not jc.settings.allow_table_value_aslist_dict:
            # pass only str
            if isinstance(datavalue,(list,dict)):
                continue

        # debug
        #print('<--|1',datapath_list, datavalue)
        # if isinstance(datapath_list[-1],int):
        #     print('int')
        # print('type of datavalue',type(datavalue))

        # loop through the entire tables in the json config.
        for tab in jc.tables:


            # only pass jsondata if the path is greater than base_path
            # otherwise continue
            if len(datapath_list) <= len(tab.base_path_list):
                if not isinstance(datavalue,str):
                    continue
            # if a list of string found then add __item to the path
            # this will keep an associative data structure being passed along the way
            # note. only an associative data structure (eg. key/value) can be transformed into a table
            if isinstance(datapath_list[-1],int) and isinstance(datavalue,str):
                #print('  ','*****')
                datapath_list.append("__item")
                #print('<--|2',datapath_list, datavalue)
                # then add item, but has a non empty str
                # if len(datavalue) > 0:
                #     datapath_list.append("__item")

            # check if the base_path matches
            #print('-->base_path',tab.base_path_list)
            base_path_match = match_two_paths(tab.base_path_list,datapath_list)
            # print(tab.base_path_list)
            # quit()

            if base_path_match:
                # if matches then
                # check changes to the current slice
                # if it changed then set has_row_completed to True,
                # meaning the making of a row has complete
                if tab.current_slice != None:
                    if datapath_list[len(tab.base_path_list)-1] != tab.current_slice:
                        # if len(tab.referencing_columns) > 0:
                        #     print ('--- slice changed for {} --> {}  ---\n'.format(tab.name,datapath_list))
                        # else:
                        #     print ('--- slice changed for {} --> {}  ---'.format(tab.name,datapath_list))
                        # quit()
                        tab.has_row_completed = True
                        tab.current_slice = datapath_list[len(tab.base_path_list)-1]

                        # check if this table is a referenced table
                        # which meant the tab.referencing_tables is greater than zero
                        if len(tab.referencing_tables) > 0:
                            for rt in tab.referencing_tables:
                                for jctab in jc.tables:
                                    if rt == jctab.name:
                                        jctab.current_slice = -1

                # set current slice
                try:
                    tab.current_slice = datapath_list[len(tab.base_path_list)-1]
                except:
                    pass

            # print(' tab.current_slice:',tab.current_slice,'------------------------------------------')
            # print(' tablename:',tab.name)
            # print('  pathtoslice:',tab.base_path_list)
            # base_path_match_test  = match_two_paths(tab.base_path_list,datapath_list)
            # print('  base_path_match_test',base_path_match_test)
            # print('  current_slice',tab.current_slice)
            # print('  lenpathtoslice',len(tab.base_path_list))

            # if the making of a row complete
            # then transform the row into a dict and append the dict to tab.rows
            if tab.has_row_completed == True:
                # start debugging
                # ---- data injection-------
                if data_injection_lod is not None:
                    # for each dict in the lod
                    for _dict in data_injection_lod:
                        # if table name matches
                        if tab.name == _dict["table_name"]:
                            # then inject it
                            dc = Data_Cell("injection",_dict["table_name"],_dict["column_name"],_dict["value"])
                            tab.current_row.append(dc)
                # ---- end data injection --

                # add data row
                if len(tab.current_row) > 0:
                    datarow = Data_Row(tab.current_row)
                    tab.rows[len(tab.rows)] = datarow



                # add data row for childs
                if len(tab.child_table_list) > 0:
                    # print('hello',tab.name,tab.child_table_list)
                    for childtab in tab.child_table_list:
                        tbl_idx= next((i for i, item in enumerate(jc.tables) if item.name == childtab), -1)

                        # # also check any referencing tables
                        # if len(jc.tables[tbl_idx].referencing_columns) > 0:
                        #     #print('hallo',tab.name,jc.tables[tbl_idx].name,jc.tables[tbl_idx].referencing_columns)
                        #     jc.tables[tbl_idx].set_referenced_values(jc)

                        if len(jc.tables[tbl_idx].current_row) > 0:
                            datarow = Data_Row(jc.tables[tbl_idx].current_row)
                            jc.tables[tbl_idx].rows[len(jc.tables[tbl_idx].rows)] = datarow
                            # reset
                            jc.tables[tbl_idx].current_row = []
                            jc.tables[tbl_idx].has_row_completed = False



                # end debugging

                #--- set values for any referencing colums ---
                # print(tab.name)
                # print(tab.referencing_columns)
                # if len(tab.referencing_columns) > 0:
                #     # print('hello',tab.name,tab.child_table_list)
                #     tab.set_referenced_values(jc)

                #print([str(x) for x in tab.current_row])
                # quit()

                # set/control states
                tab.current_row = [] # reset
                tab.has_row_completed = False


            for jcpath in tab.jsonpath:
                #print(' -->',tab.name,jcpath)

                # match the two paths
                jcpath_list = jcpath.split("/")
                if len(jcpath_list) == len(datapath_list):
                    match = match_two_paths(jcpath_list,datapath_list)
                    #print(' ->',match)
                    if match: # then add to row_data
                        tab.current_datapath = datapath_list
                        # if this is the first cell of the curent row
                        if len(tab.current_row) == 0:
                            # then tag a record from datapath_list, this will create table relationship
                            pathids = tab.gen_pathids_fullpath()
                            # print(pathids)
                            # quit()
                            for _pi in pathids:
                                for k,v in _pi.items():
                                    dc = Data_Cell(k,tab.name,k,v)
                                    tab.current_row.append(dc)

                        # if datavalue is a list or dict but user doesn't want it be in a table
                        # then convert the list as string
                        # however this will be valid only for path further than base_path
                        if jc.settings.allow_table_value_aslist_dict:
                            if isinstance(datavalue,list):
                                if len(datapath_list) > len(tab.base_path_list):
                                    datavalue = str(datavalue)
                            if isinstance(datavalue,dict):
                                if len(datapath_list) > len(tab.base_path_list):
                                    datavalue =  str(datavalue)

                        dc = Data_Cell(tab.name,tab.name,jcpath,datavalue)
                        tab.current_row.append(dc)


    # the making of the last row complete
    for tab in jc.tables:
        # start debug
        # print(tab.name)

        # ---- data injection-------
        if data_injection_lod is not None:
            # for each dict in the lod
            for _dict in data_injection_lod:
                # if table name matches
                if tab.name == _dict["table_name"]:
                    # then inject it
                    dc = Data_Cell("injection",_dict["table_name"],_dict["column_name"],_dict["value"])
                    tab.current_row.append(dc)
        # ---- end data injection --

        #print('hello',tab.name,tab.child_table_list)

        if len(tab.current_row) > 0:
            datarow = Data_Row(tab.current_row)
            tab.rows[len(tab.rows)] = datarow

        # end debugging

        # # --- set values for any referencing colums to any child tables---
        # if len(tab.child_table_list) > 0:
        #     #print('hello',tab.name,tab.child_table_list)
        #     for childtab in tab.child_table_list:
        #         tbl_idx= next((i for i, item in enumerate(jc.tables) if item.name == childtab), -1)
        #
        #         # also check any referencing tables
        #         if len(jc.tables[tbl_idx].referencing_columns) > 0:
        #             #print('hallo',tab.name,jc.tables[tbl_idx].name,jc.tables[tbl_idx].referencing_columns)
        #             jc.tables[tbl_idx].set_referenced_values(jc)

        # --- set value for any referencing columns to this table
        # if len(tab.referencing_columns) > 0:
        #     tab.set_referenced_values(jc)

    return jc

    #print('---end debugging...' + '\n')
