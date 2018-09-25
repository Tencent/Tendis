#!/usr/bin/env python

import subprocess
import os
import time
from util import log_notice
from util import rand_string_alpha

class RedisServer(object):
    def __init__(self, port, path):
        self.port = port
        self.path = path + rand_string_alpha(6)

    def setup(self, valgrind):
        os.makedirs(self.path)
        os.makedirs(self.path + "/db")
        os.makedirs(self.path + "/log")
        f = open(self.path + "/test.cfg", "w")
        f.write("bind 127.0.0.1\n")
        f.write("port " + str(self.port)+ "\n")
        f.write("loglevel debug\n")
        f.write("logdir ./" + self.path + "/log\n")
        f.write("storage rocks\n")
        f.write("dir ./" + self.path + "/db\n")
        f.write("rocks.blockcachemb 4096\n")
        f.write("pidfile " + self.path + "/tendisplus.pid\n")
        f.close()
        if valgrind:
            ret = subprocess.call(["valgrind", "--tool=memcheck", "--leak-check=full",
                "./tendisplus", self.path + "/test.cfg"])
        else:
            ret = subprocess.call(["./tendisplus", self.path + "/test.cfg"])
        if ret != 0:
            raise RuntimeError("startup failed")
        for i in xrange(30):
            if os.path.isfile(self.path + "/tendisplus.pid"):
                return
            else:
                print "waiting for pidfile"
                time.sleep(1)
        raise RuntimeError("wait for pidfile timeout")

    def shutdown(self):
        raise Exception("not impl")
