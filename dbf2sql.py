#!/usr/bin/python3

######################################################
#
# Brandon Fairburn 8/1/2017
#
# dbf2sql.py
#   Converts an old FoxPro file to a PostgreSQL table
#
######################################################


########################################################################################################
# Initial setup
########################################################################################################

import argparse

clargs_p = argparse.ArgumentParser()
clargs_p.add_argument('FILE', help='the path to the DBF file for conversion')
clargs_p.add_argument('-v', '--verbose', help='show all SQL commands', action='store_true')
clargs = vars(clargs_p.parse_args())

# using ordered dictionary to maintain column order
from collections import OrderedDict

# yield all the bytes from the dbf file
def bytes_from_file(filename, chunksize=8192):
    with open(filename) as f:
        while True:
            chunk = f.read(chunksize)
            if chunk:
                for b in chunk:
                    yield b

            else:
                break

# convert a byte array from file to an integer
def arr2i(a):
    #least significant byte first
    num = 0
    for i in range(len(a)):
        num = num | (a[i] << (8 * i))

    return num

print('working...')

# raw data from file
raw_arr = list(bytes_from_file(clargs['FILE']))

# integer values
arr = [ord(b) for b in raw_arr]

# DBF file structure:
# https://msdn.microsoft.com/en-us/library/aa975386(v=vs.71).aspx
first_record = arr2i(arr[8:10])
num_records = arr2i(arr[4:8])
record_length = arr2i(arr[10:12])

#_______________________________________________________________________________________________________


########################################################################################################
# Fields
########################################################################################################

# get the field data (columns)
fields = OrderedDict()

class Field:
    def __init__(self, name, t, disp, length):
        self.name = name
        self.t = t
        self.disp = disp
        self.length = length

    def __str__(self):
        return  '[FIELD] name: {}, type: {}, displacement: {}, length: {}'\
            .format(self.name, self.t, self.disp, self.length)


# field subrecords (info like field name, length, etc.)
cur = 32 
while raw_arr[cur] != '\n':
    name = ''.join(raw_arr[cur:cur+10]).replace(chr(0), '')
    t = raw_arr[cur+11]
    disp = arr2i(arr[cur+12:cur+16])
    length = arr[cur+16]

    # some SQL keywords we have to avoid
    # add more to kwords if we find more conflicts
    kwords = ('group')
    if name.lower() in kwords:
        name = "M{}".format(name)

    fields[name] = Field(name, t, disp, length)
    cur += 32

# now we have all the fields, the number of records, and the length of each record
#_______________________________________________________________________________________________________


########################################################################################################
# Records
########################################################################################################

# move the cursor to the first record
cur = first_record

# get records (rows in table)
records = list()
for i in range(num_records):
    row = OrderedDict()

    # store data from each column in the row
    for fname in fields:

        # the start and end indexes for this field
        start = cur + fields[fname].disp
        end = start + fields[fname].length

        # get data from the column as a string
        row[fname] = ''.join(raw_arr[ start : end ]).strip()

        # if the data is numeric
        if fields[fname].t == 'N':
            row[fname] = int(row[fname])

    # append the row to the list and move the cursor to the next record
    records.append(row)
    cur += record_length

# now we have all the record data, ready to generate the SQL
#_______________________________________________________________________________________________________


########################################################################################################
########################################################################################################
########################################################################################################
# SQL stuff

# function for executing SQL (to allow verbose mode)
def sqlexec(c, s):
    if clargs['verbose']:
        print(s)
    c.execute(s)

import configparser

try:
    import psycopg2
    import psycopg2.extras 
except:
    try:
        print("couldn't find postgres module, trying to install it now...")
        import pip
        pip.main(['install', 'psycopg2'])

        import psycopg2
        import psycopg2.extras
    except:
        print("couldn't load postgres module... try running `pip install psycopg2` with admin rights")
        print('or run this script again with admin rights')
        exit(1)

# read the settings
try:
    cfg = configparser.ConfigParser()
    cfg.read('settings.ini')
    database = str(cfg.get('database', 'database'))
    host = str(cfg.get('database', 'host'))
    port = str(cfg.get('database', 'port'))
    user = str(cfg.get('database', 'user'))
    password = str(cfg.get('database', 'password'))
except:
    print('error reading settings.ini')
    exit(1)

# try to connect to the database
try:
    conn = psycopg2.connect(database=database, host=host, port=port, user=user, password=password)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
except:
    print('error connecting to database, check settings.ini')
    exit(1)

    
########################################################################################################
# Create table
########################################################################################################


# table name (from input file)
table_name = input('name of new table in database: ')#clargs['FILE'].split('.')[0]

# drop the table if it already exists
sql = "DROP TABLE IF EXISTS {};".format(table_name)
sqlexec(cur, sql)


# create the table
sql = "CREATE TABLE {} (".format(table_name)
for fname in fields:
    sql += "{} {},".format(fname.lower(), "int" if fields[fname].t == 'N' else "text")
sql = "{});".format(sql[ 0 : len(sql) - 1 ]) # remove the last comma

sqlexec(cur, sql)
#_______________________________________________________________________________________________________


########################################################################################################
# Insert rows to table
########################################################################################################

for row in records:
    sql = "INSERT INTO {} VALUES(".format(table_name)
    for fname in row:
        # text data needs 'single quotes' around it, so handle it differently
        
        # text data
        if fields[fname].t == 'C':
            sql += "'{}',".format(row[fname])

        # numeric data
        else:
            sql += "{},".format(row[fname])
    
    sql = "{});".format(sql[ 0 : len(sql) - 1 ]) # remove the last comma
    sqlexec(cur, sql)
#_______________________________________________________________________________________________________

# commit changes and exit
conn.commit()
print('done')
