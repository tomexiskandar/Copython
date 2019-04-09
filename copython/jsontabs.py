import json

def match_two_paths(jcpath,datapath):
    res = False
    #if len(jcpath) == len(datapath):
    matches = 0
    tup = list(zip(jcpath,datapath))
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




class Fly_Table():
    def __init__ (self,name):
        self.name = name
        self.is_primary = False
        self.path_toslice = None
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



    def gen_pathids(self):
        #example of datapath 	    ['Results', 0, 'Manufacturers', 1, 'Address', 'AddressLine1']
        #example of path_toslice 	['Results', '#', 'Manufacturers', '#']
        #example of return          [{Result=0},{Manufacturers=1}]
        pathids = []
        for i,p in enumerate(self.path_toslice):
            if p != "#":
                _pdict = {p:self.current_datapath[i+1]}
                pathids.append(_pdict)
        return pathids



    def set_referenced_values(self,jc):
        #print(self.name,self.referencing_columns)
        pathids = self.gen_pathids()

        # update tab.current_row
        rc_with_values = self.referencing_columns
        for rc in rc_with_values:
            for _datacell in self.current_row:
                if rc["referenced_columnname"] == _datacell.column_name:
                    rc["referenced_value"] = _datacell.value
                    break

        # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # print('rc_with_values:',rc_with_values)
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
                    # print('--->p',p)
                    for k,v in p.items():
                        for dc in row.datarow:
                            # if this dc is a referenced table
                            if dc.data_source != dc.table_name:
                                # then match the data_source and value
                                # print(dc)
                                if k == dc.data_source and v == dc.value:
                                    # print('-->match',dc)
                                    matches += 1
                if matches == len(pathids):
                    dc = Data_Cell(self.name,self.name,_rcv["referenced_columnname"],_rcv["referenced_value"])
                    row.datarow.append(dc)
                    self.counter += 1
                    if self.is_primary:
                        jc.tables[tbl_idx].last_ref_updates_rowid = rowid

                    # tag row that is already process, so the loop is shorter when it repeats


            #update the current_row
            cr_matches = 0
            for p in pathids:
                for k,v in p.items():
                    for dc in jc.tables[tbl_idx].current_row:
                        # if this dc is a referenced table
                        if dc.data_source != dc.table_name:

                            if k == dc.data_source and v == dc.value:
                                # then match the data_source and value
                                # print('------match-----',dc)
                                cr_matches += 1
            if cr_matches == len(pathids):
                dc = Data_Cell(self.name,self.name,_rcv["referenced_columnname"],_rcv["referenced_value"])
                jc.tables[tbl_idx].current_row.append(dc)
                self.counter += 1


class JSONPath_Tables_Conf():
    def __init__(self,path):
        self.path = path
        self.jsondata = None
        self.tables = []
        self.intertables_column_copies = []
        self.primary_table = None
        self.get_json_fromfile(self.path)
        self.create_tables()
        self.set_primary_table()

    def get_json_fromfile(self,path):
        print('parsing json file "{}"'.format(path))
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
            flytab.path_toslice = tab["path_toslice"].split('/')
            for jp in tab["jsonpath"]:
                _jplist = jp.split("/")
                flytab.jsonpath.append(_jplist)
            if 'columns_tocopy' in tab:
                flytab.columns_tocopy = tab['columns_tocopy']
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
            tup = (len(tab.path_toslice),tab.name)
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
                # yield only non dict or list
                if not isinstance(v,(dict,list)):
                    yield k, v

def gen_json_tables(jc,data,data_injection_lod = None):
    for datapath, datavalue in traverse(data):
        if isinstance(datavalue,(list,dict)):
            continue
        # datavalue_toprint = 'the whole list!!!!' if isinstance(datavalue,list) else datavalue
        # datavalue = datavalue if datavalue else 'None'

        #print(datapath, datavalue_toprint)

        # loop through the entire tables in the json config.
        for tab in jc.tables:
            # check if the path_toslice matches
            #print('-->path_toslice',tab.path_toslice)
            path_toslice_match = match_two_paths(tab.path_toslice,datapath)

            if path_toslice_match:
                # if matches then
                # check changes to the current slice
                # if it changed then set has_row_completed to True,
                # meaning the making of a row has complete
                if tab.current_slice != None:
                    if datapath[len(tab.path_toslice)-1] != tab.current_slice:
                        # if len(tab.referencing_columns) > 0:
                        #     print ('--- slice changed for {} --> {}  ---\n'.format(tab.name,datapath))
                        # else:
                        #     print ('--- slice changed for {} --> {}  ---'.format(tab.name,datapath))
                        # # quit()
                        tab.has_row_completed = True
                        tab.current_slice = datapath[len(tab.path_toslice)-1]

                        # check if this table is a referenced table
                        # which meant the tab.referencing_tables is greater than zero
                        if len(tab.referencing_tables) > 0:
                            for rt in tab.referencing_tables:
                                for jctab in jc.tables:
                                    if rt == jctab.name:
                                        jctab.current_slice = -1

                # set current slice
                try:
                    tab.current_slice = datapath[len(tab.path_toslice)-1]
                except:
                    pass

            # print(' tab.current_slice:',tab.current_slice,'------------------------------------------')
            # print(' tablename:',tab.name)
            # print('  pathtoslice:',tab.path_toslice)
            #path_toslice_match = match_two_paths(tab.path_toslice,datapath)
            # print('  path_toslice_match',path_toslice_match)
            # print('  current_slice',tab.current_slice)
            # print('  lenpathtoslice',len(tab.path_toslice))

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

                datarow = Data_Row(tab.current_row)
                tab.rows[len(tab.rows)] = datarow

                # end debugging

                # _row_dict = tab.gen_row_dict()
                # tab.rows.append(_row_dict)

                #--- set values for any referencing colums ---
                if len(tab.referencing_columns) > 0:
                    tab.set_referenced_values(jc)

                #print([str(x) for x in tab.current_row])
                # quit()

                # set/control states
                tab.current_row = [] # reset
                tab.has_row_completed = False


            for jcpath in tab.jsonpath:
                #print(' ->',jcpath)
                # match the two paths
                if len(jcpath) == len(datapath):
                    match = match_two_paths(jcpath,datapath)
                    #print(' ->',match)
                    if match: # then add to row_data
                        tab.current_datapath = datapath
                        # if this is the first cell of the curent row
                        if len(tab.current_row) == 0:
                            # then tag a record from datapath, this will create table relationship
                            pathids = tab.gen_pathids()
                            pathids_datacell = []
                            for _pi in pathids:
                                for k,v in _pi.items():
                                    dc = Data_Cell(k,tab.name,k,v)
                                    tab.current_row.append(dc)
                        dc = Data_Cell(tab.name,tab.name,datapath[-1],datavalue)
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


        datarow = Data_Row(tab.current_row)
        tab.rows[len(tab.rows)] = datarow
        # end debugging

        # --- set values for any referencing colums ---
        if len(tab.referencing_columns) > 0:
            tab.set_referenced_values(jc)

    return jc

    #print('---end debugging...' + '\n')
