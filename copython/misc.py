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
c = 'c'
my_dict = {'z':1,'b':2,c:4}
print(my_dict)
with open(r"E:\data\test_json.json","w",encoding="utf-8") as f:
    json.dump(my_dict,f,indent=4,sort_keys=True)





