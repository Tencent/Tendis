import string
import random
from datetime import datetime

def log_notice(msg, format):
    print (str(datetime.now()) + " MSG: " + msg) % format

def log_error(msg, format):
    print (str(datetime.now()) + " ERR: " + msg) % format

def rand_string_alpha(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))
    
def rand_string_binary(max):
    c = 0
    string = []
    size = int(max * random()) 
    if size < 4:
      return int(max * random())

    for i in range(size):
        c = "%c"%(int(255 * random()),)
        string.append(c)

    return "".join(string)
    

def rand_int(max):
    return int(random() * max)

FNV_64_INIT = 0xcbf29ce484222325
FNV_64_PRIME = 0x100000001b3

def fnv1a_64(key):
    hash = int(FNV_64_INIT)
    for x in range(len(key)):
        val = ord(key[x])
        hash = hash ^ val
        hash = hash * (0xffffffff & FNV_64_PRIME)
        
    return hash & 0xffffffff
