import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "copython",
    version = "0.0.1",
    author = "Tomex Iskandar",
    author_email = "tomex.iskandar@gmail.com",
    description = ("copy data in/out of database"),
    license = "MIT",
    keywords = "copython copy data using python",
    url = "https://github.com/tomexiskandar/copython",
    packages=['copython'],
    install_requires=['pyodbc'],
    long_description=read('README.txt'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: ETL",
        "License :: OSI Approved :: MIT License",
    ],
)
