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

import json
cf = r"E:\Documents\Visual Studio 2017\Projects\copython\test\_cf_load_csv_into_mssql.json"
data = json.load(open(cf))
print(data)
desc = data["description"]
id = data["copy"]["id"]
path = data["copy"]["source"]["path"]
colmap2 = data["copy"]["column_mapping"][2]["source"]
print(desc)
print(id)
print(path)
print(colmap2)



