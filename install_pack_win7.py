"""
This is my codes to install copython package in my Windows 7.
I must run visual studio as administrator in order to run this code properly.
The reason is the package in my system is stored in the following windows system:
C:\Program Files\Python36\Lib\site-packages
"""
import sys
from setuptools import sandbox
import subprocess

# create/replace a distribution and will be stored in folder dist of copython root folder
sandbox.run_setup('setup.py', ['sdist', '--format=zip'])

# check if copython package already installed or not
# so we can do uninstall first or not
try:
    import copython
    subprocess.call('pip uninstall copython')
except ImportError:
    print('new installation...')

# install copython
subprocess.call('pip install "E:\Documents\Visual Studio 2017\Projects\copython\dist\copython-0.0.1.zip"')
    




 