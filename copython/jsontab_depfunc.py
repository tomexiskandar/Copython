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


# for tab in jc.tables:
#     if len(tab.columns_tocopy) > 0:
#         #for coltc in jt.tables[1].columns_tocopy:
#         for coltc in tab.columns_tocopy:
#             #print(tab.name,'columns tocopy',coltc)
#             #loop through target and source table and do the matching
#             tbl_idx_s= next((i for i, item in enumerate(jc.tables) if item.name == coltc["tablename"]), -1)
#             # print(tbl_idx_s)
#             # quit()
#             jc.tables[tbl_idx_s].last_ref_updates_rowid = 0
#             ########## target table ##########
#             #for idx_t, row_t in jt.tables[1].rows.items():
#             for idx_t, row_t in tab.rows.items():
#                 #print(' prod',idx,row.print_kv())
#                 ########## source table ##########
#                 # for idx_s, row_s in jt.tables[tbl_idx_s].rows.items():
#                 for idx_s in range(jc.tables[tbl_idx_s].last_ref_updates_rowid,len(jc.tables[tbl_idx_s].rows)):
#                     # match the target and the source based upon provided keys
#                     row_s = jc.tables[tbl_idx_s].rows[idx_s]
#                     matches = 0
#                     for k in coltc["keys"]:
#                         # print('key ',k)
#                         if row_t.get_value(k) == row_s.get_value(k):
#                             matches += 1
#                             # print('! matching...')
#                     if matches == len(coltc["keys"]):
#                         # this is a match! so make a copy
#                         # print('!!!! a match')
#                         colname_toadd = '@' + coltc["columnname"] #prefix with @ to differentiate with jsonpath
#                         dc = jsontabs.Data_Cell(jt.tables[tbl_idx_s].name,tab.name,colname_toadd,row_s.get_value(coltc["columnname"]))
#                         row_t.add_dc(dc)
#
#                         #flag last row
#                         jc.tables[tbl_idx_s].last_ref_updates_rowid = idx_s
