"""
To improve my testing productivity, i need to organise all testing files in test folder.
I create var use_package to control what kind of codes i'm testing, for eg.
if the value is False, I'm executing copython codes from my working folder, while True
I'm executing copython codes from the site_packages
You should remove the four lines below .
"""
use_package = False
if use_package is False:
    import sys
    sys.path.insert(0,r"e:\documents\visual studio 2017\projects\copython")
    
def progress():
    print('|',end='',flush=True)
import os.path
import json
unparsed_json = r"E:\Documents\Visual Studio 2017\Projects\copython\test\_cf_load_csv_into_mssql2.json"
#unparsed_json = '{"a":1,"b":2}'
data = None
if os.path.isfile(unparsed_json):
    try:
        cc_json = json.load(open(unparsed_json))
    except Exception as e:
        print(e)
else:
    try:
        cc_json = json.loads(unparsed_json)
    except Exception as e:
        print(e)
    


#print(data)
#for k,v in cc_json.items():
#    print("----")
#    #print(k,"->",v)
#    print(type(v))
#    if type(v) == str:
#        print(k,"->",v)
#    if type(v)==dict:
#        print("v is a dictionary")
#        # go to its first child
#        for k,v in v.items():
#            if type(v) == str:
#                print(k,"->",v)


#print('--------')
#for key in cc_json.keys():
#    print(key)

copies = cc_json["copy"]

for c in copies:
    #print id
    print(type(c))
    print(c["id"])
    #print(c)
    for k in c.keys():
        print(k)
    break

quit()
try:
    desc = data["description"]
    id = data["copy"]["id"]
    path = data["copy"]["source"]["path"]
    colmap2 = data["copy"]["column_mapping"][2]["source"]
    quotechar = data["copy"]["source"]["quotechar"]
    print(desc)
    print(id)
    print(path)
    print(colmap2)
    print(quotechar)
except KeyError as e:
    print(e.__class__.__name__,e)
    for k,v in e.__dict__.items():
        print(k,v)




