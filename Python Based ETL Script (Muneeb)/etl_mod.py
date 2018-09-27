import petl as etl
import sys
import ConfigParser
import pymysql.cursors 
import xml2json
import optparse
import json
from dateutil.parser import parse
#import MySQLdb

#converting dictionary object to list
def d2l(tb):
    l = list()
    l.append(tb[1].keys())
    t = [x.values() for x in tb[1:]]
    l = l + t
    return l

#checking if a given string is date or not
def isdate(string):
    try: 
        parse(string)
        return 1
    except ValueError:
        return 0

#checking if a given string is float or not
def isfloat(a):
    try:
        float(a)
        return 1
    except ValueError:
        return 0

#checking if a given string is int or not
def isint(a):
    try:
        int(a)
        return 1
    except ValueError:
        return 0

#Mapping the attributes with the datatypes
def dmap(attr,row):
    dtype = dict()
    for i in range(0,len(row)):
        if(isint(row[i])==1):
            dtype[attr[i]] = "INT(20)"
        elif(isfloat(row[i])==1):
            dtype[attr[i]] = "FLOAT(20)"
        else:
            dtype[attr[i]] = "VARCHAR(100)"
    return dtype
    """elif (isdate(row[i])==1):
            dtype[attr[i]] = "DATETIME" """

#Generates create table query. Arguments are attributes, datatype mapping dict and table object
def crtTable(attr,dtype,tb):
    q = "create table " + tb + "("
    for i in range(0,len(attr)-1):
        q = q + " " + attr[i] + " " + dtype[attr[i]] + ","
    q = q + " " + attr[len(attr)-1] + " " + dtype[attr[len(attr)-1]] + ");" 
    return q
        
Config = ConfigParser.ConfigParser() #ConfigParser object
Config.read("config.ini") #reads the config file

#To read from the config file the details of the database.
def ConfigSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            #print option
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

#returns insert query string. Arguments are the values to be inserted and attribute list.
def query_i(row,attr):
    q = "INSERT INTO " + tname + " (" 
    for i in range(0,len(attr)-1):
        q = q + attr[i] + ", "
    q = q + attr[len(attr)-1] + ") values("
    for i in range(0,len(row)-1):
        q = q + " '" + str(row[i]) + "',"
    q = q + " '" + str(row[len(row)-1]) + "');"
    return q

#returns update query string. Arguments are the updates values, attribute list and "where" clause argument
def query_u(row,attr,pkey,search):
    q = "UPDATE " + tname + " SET " 
    for i in range(0,len(attr)-1):
        q = q + attr[i] + "= '" + str(row[i]) + "',"
    q = q + attr[len(attr)-1] + "= '" + str(row[len(row)-1]) + "' where " + pkey +  "= '" + search + "';"
    return q
        
#Extract the data from a file. Returns a two dimensional list or table object. Arguments: file path
def extract(file):
    tb = list()
    if file == "sql":
        host = raw_input("Enter Host:")
        user = raw_input("Enter Username:")
        pwd = raw_input("Enter pwd:")
        dtb = raw_input("Enter Database Name:")
        table = raw_input("Enter Table Name:")
        conn = pymysql.connect(host=host,
                               user=user,
                               password=pwd,                            
                               db=dtb,
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)
        temp = etl.fromdb(conn, "SELECT * FROM " + table) 
        tb = d2l(temp)  
    elif ".csv" in file:
        tb = etl.fromcsv(file)
    elif ".xlsx" in file:
        tb = etl.fromxls(file)
    elif ".json" in file:
        tb = etl.fromjson(file)
        print tb
    elif ".xml" in file:
        f = open(file,'r').read()
        options = optparse.Values({"pretty": True})
        jsn = json.dumps(xml2json.xml2json(f,options))
        ob = json.loads(jsn.decode('string-escape').strip('"'))
        temp = dict()
        for key in ob.keys():
            for skey in ob[key].keys():
                temp = json.dumps(ob[key][skey])
        with open("temp.json","w") as tmp:
            tmp.write(temp)
        tb = etl.fromjson("temp.json")
        print tb[0]
        #tb = etl.fromxml(file,'.//ROW',{'Service_Name':'Service_Name','Status':'Status','Service_Type':'Service_Type','Time':'Time'})
    elif ".txt" in file:
        tb = etl.fromtext(file)
        print tb
    return tb

#loads the data into MySQL database. Arguments: Table Object
def load(tb,pkey,hst,usr,pwd,dtb):
    db = pymysql.connect(host=hst,
                         user=usr,
                         password=pwd,                            
                         db=dtb,
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
    cur = db.cursor()
    
    if cur.execute("SHOW TABLES LIKE 'ETL'") == 0:
        cur.execute(crtTable(tb[0],dmap(tb[0],tb[1]),tname))
    print "No. of data to be processed: "+ str(len(tb))
    cur.execute('SET SQL_MODE=ANSI_QUOTES')
    etl.todb(tb,db,tname);
    print "Done loading."
    db.commit()

    """s = int()
    #ins = 0
    #update = 0
    #print pkey
    #typ = dtype(tb[0],tb[1])
    for i in range(1,len(tb[0])):
        if tb[0][i] == pkey:
            s = i
            break
    for i in range(1,len(tb)):
        result = cur.execute("select * from " + tname + " where " + pkey + " = '" + tb[i][s] + "';")
        if result == 0:
            cur.execute(query_i(tb[i],tb[0]))
            print "Query OK (I) [" + str(i) + "]"
            #ins = ins + 1
        else:
            cur.execute(query_u(tb[i],tb[0],pkey,tb[i][s]))
            print "Query OK (U) [" + str(i) + "]"
            #update = update + 1
        #print str(ins) + " " + str(update)
        db.commit() """
        
host = ConfigSectionMap("Database")['host'] 
user = ConfigSectionMap("Database")['user']
passwd = ConfigSectionMap("Database")['password']
db = ConfigSectionMap("Database")['database']
pkey = ConfigSectionMap("Table")['pkey'] #Primary Key of the table
tname = ConfigSectionMap("Table")['tname'] #Table name in the database
file = sys.argv[1] #File name passed as an argument in command line
 
tb = extract(file) #store the list in a variable
load(tb,pkey,host,user,passwd,db) #load the data in the database.