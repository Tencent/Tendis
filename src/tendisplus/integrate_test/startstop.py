#!/usr/bin/env python

import subprocess
import os
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
        f.close()
        if valgrind:
            ret = subprocess.call(["valgrind", "--tool=memcheck", "--leak-check=full",
                "./tendisplus", self.path + "/test.cfg"])
        else:
            ret = subprocess.call(["./tendisplus", self.path + "/test.cfg"])
        return ret

    def shutdown(self):
        raise Exception("not impl")
