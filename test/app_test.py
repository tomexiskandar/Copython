import re
import os
from datetime import datetime


proxy_dict = None

def set_proxy_info(username,password):
    http_proxy  = "http://{}:{}@webproxy.gslb.health.nsw.gov.au:8080".format(username,password)
    https_proxy = "https://{}:{}@webproxy.gslb.health.nsw.gov.au:8080".format(username,password)
    #ftp_proxy   = "ftp://10.10.1.10:3128"

    proxy_dict = {
                  "http"  : http_proxy,
                  "https" : https_proxy
                  #"ftp"   : ftp_proxy
                }
    return proxy_dict

class Dump:
    """plain data dump"""
    def __init__(self,dirname,data, name=None):
        self.dirname = dirname
        self.data = data
        self.name = name
        #self.totext(dirname,data,name)
    def totext(self):
        timestamp = str(datetime.now())
        timestamp = re.sub("[-:.]","",timestamp)
        if self.name is not None:
            filename = "dump_" + self.name
        else:
            filename = "dump"
        path = os.path.join(self.dirname,filename + timestamp + ".txt")
        try:
            file = open(path,"w")
            if type(self.data) is list or type(self.data) is tuple:
                for row in self.data:
                    try:
                        file.write(str(row)+ '\n') #
                    except Exception as e:
                        print('warning! {} with reference to writing row {}.'.format(e,row))
            else:
                file.write(str(self.data))
        except Exception as e:
            print('warning! {} with reference to dumping object.'.format(e))
            pass

    def tocsv(self):
        timestamp = str(datetime.now())
        timestamp = re.sub("[-:.]","",timestamp)
        if self.name is not None:
            filename = "dump_" + self.name
        else:
            filename = "dump"
        path = os.path.join(self.dirname,filename + timestamp + ".csv")
        try:
            file = open(path,"w")
            if type(self.data) is list or type(self.data) is tuple:
                i = 0
                for row in self.data:
                    columnname_list = list(row.keys())
                    data_list = list(row.values())
                    try:
                        if i == 0: #columns and data
                            file.write(','.join(columnname_list) + '\n')
                            file.write(','.join(str(x) for x in data_list) + '\n')
                        if i > 0: #data only
                            file.write(','.join(str(x) for x in data_list) + '\n')
                    except Exception as e:
                        print('warning! {} with reference to writing row {}.'.format(e,row))

                    i += 1
            else:
                file.write(str(self.data))
        except Exception as e:
            print('warning! {} with reference to dumping object.'.format(e))
            pass
