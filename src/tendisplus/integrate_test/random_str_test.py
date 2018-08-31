import threading
import time
import redis
import string
import random

def randomstr(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))
    
def random_pipelined_query():
    while True:
        r = redis.Connection(host='127.0.0.1', port=8903)
        pipe_cnt = random.randint(1, 200)
        for i in xrange(pipe_cnt):
            r.send_command(randomstr(size=random.randint(1,500)), randomstr(size=random.randint(1,4096)))
        for i in xrange(pipe_cnt):
            s = r.read_response()
            print s

for i in xrange(1):
    threading.Thread(target=random_pipelined_query).start()

time.sleep(3600)
