copython
This program is intended for data analyst, data scientist, data engineer and data warehouse specialist. By this time of writing this file, here the facts:

copython.py is the interface that users/clients can use
some testings* done and worked as expected.

*Some testings: Testings are done with the following servers editions with installed ODBC drivers:

SQL Server version 12.0.200 year 2014 64 bit. ODBC driver 11 for SQL Server
PostgreSQL version 10.1 year 2017 64 bit. ODBC driver PostgreSQL Unicode(x64)
Oracle version 11g Express Edition year 2017 64 bit. ODBC driver Oracle in instantclient_12_2, Oracle in XE
MySQL version 5.7.20 year 2017 64 bit. ODBC driver MySQL ODBC 5.3 Unicode Driver


Successfull copy operations as the following:

copy csv file into SQL Server
copy csv file into PostgreSQL
copy SQL Server table to PostgreSQL table and vice versa
copy SQL Server select query to PostgreSQL table and vice versa
copy MySQL table and sql query to SQL Server table
copy Oracle table to SQL Server table
copy Oracle sql query to SQL Server table
