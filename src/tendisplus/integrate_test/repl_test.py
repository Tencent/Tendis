#!/usr/bin/env python

import time
import redis
from startstop import RedisServer

def test_repl1(m, s):
    time.sleep(3)
    cli1 = redis.Connection(host="127.0.0.1", port=12345)
    cli2 = redis.Connection(host="127.0.0.1", port=12346)

    cli1.send_command("set", "a", "1")
    assert(cli1.read_response() == "OK")
    cli1.send_command("set", "b", "2")
    assert(cli1.read_response() == "OK")

    cli2.send_command("slaveof", "127.0.0.1", "12345")
    print cli2.read_response()
    time.sleep(1)
    cli2.send_command("get", "a")
    assert(cli2.read_response() == "1")
    cli2.send_command("get", "b")
    assert(cli2.read_response() == "2")

def test_repl():
    m = RedisServer(12345, "m_")
    s = RedisServer(12346, "s_")
    assert(m.setup(False) == 0)
    assert(s.setup(False) == 0)
    test_repl1(m, None)

if __name__ == '__main__':
    test_repl()
