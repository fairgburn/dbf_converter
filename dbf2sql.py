#!/usr/bin/python3


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

# raw data from file
raw_arr = list(bytes_from_file('sys.dbf'))

# integer values
arr = [ord(b) for b in raw_arr]

# DBF file structure:
# https://msdn.microsoft.com/en-us/library/aa975386(v=vs.71).aspx
first_record = arr2i(arr[8:10]) + 1 #      ((arr[8]) | (arr[9] << 8)) + 1
num_records = arr2i(arr[4:8])   #  (arr[7] << (8*3)) | (arr[6] << (8*2)) | (arr[5] << 8) | arr[4]
record_length = arr2i(arr[10:12])#       (arr[11] << 8) | arr[10]


# get the field data (columns)
fields = dict()
class Field:
    def __init__(self, name, t, disp, length):
        self.name = name
        self.t = t
        self.disp = disp
        self.length = length

    def __str__(self):
        return  '[FIELD] name: {}, type: {}, displacement: {}, length: {}'\
            .format(self.name, self.t, self.disp, self.length)


cur = 32 # the byte we're reading
while raw_arr[cur] != '\n':
    name = ''.join(raw_arr[cur:cur+10]).replace(chr(0), '')
    t = raw_arr[cur+11]
    disp = arr2i(arr[cur+12:cur+16])
    length = arr[cur+16]

    fields[name] = Field(name, t, disp, length)
    cur += 32

# now we have all the fields, the number of records, and the length of each record

# move the cursor to the first record
cur = first_record

# get records (rows in table)
rows = dict()
for fname in fields:
    pass
