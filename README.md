# Python-Based-ETL-Tool

===========
Description
===========
Extract Data from different formats and load it into your MySQL database. The script currently supports four formats, namely .xml, .xlsx, .json and .csv. The script also provides an option to fork out data from another database and load it into your own.

============
Dependencies
============
The script makes use of python modules named petl from extraction and loading purposes, pymysql as a Python-MySQL connector and xml2json for converting xml file to a json object.

Download Links:
petl - https://pypi.python.org/pypi/petl
pymysql - https://pypi.python.org/pypi/PyMySQL
xml2json - https://github.com/hay/xml2json

=========
Execution
=========
The script can be executed from command line using the following command:
>> python "Path_to_script\etl_mod.py" [FILENAME/OPTIONS]

OPTIONS:
sql - to extract data from another MySQL database. The user has to enter the hostname, userid, password, database and the table name.

==========
config.ini
==========
Note: Make sure you edit the table name and pkey of the config file as per your requirement before executing the script.

[Database]
Host: #host of your datbase
User: #username
Password: #password
Database: #database name

[Table]
Tname: #name of the table in which data needs to be stored
Pkey: #primary key of the data you are about to extract

====================
Function description
====================
Included in the script itself. 
