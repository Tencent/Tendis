#!/usr/bin/env python

import time
import redis
import threading
from startstop import RedisServer

def test_repl_match2(m, s):
    running = True 
    lk = threading.Lock()
    lst = []
    thd_lst = []
    def insert(svr, idx):
        print "thread:", idx, "starts"
        cli = redis.Connection(host="127.0.0.1", port=svr.port)
        # assume base on 10000000000, never overlaps
        cnt = idx*10000000000
        while running:
            cnt += 1
            lk.acquire()
            lst.append(cnt)
            lk.release()
            cli.send_command("set", str(cnt), str(cnt))
            res = cli.read_response()
            assert(res == "OK")
        print "thread:", idx, "stops"

    for i in xrange(10):
        a = threading.Thread(target=insert, args=(m, i))
        a.start()
        thd_lst.append(a)

    time.sleep(10)
    lk.acquire()
    print "list size:", len(lst)
    lk.release()

    cli1 = redis.Connection(host="127.0.0.1", port=m.port)
    cli2 = redis.Connection(host="127.0.0.1", port=s.port)
    cli2.send_command("slaveof", "127.0.0.1", str(m.port))
    res = cli2.read_response()
    assert(res == "OK")

    time.sleep(10)
    running = False
    for o in thd_lst:
        o.join()

    cli1.send_command("binlogpos", "0")
    m_pos = int(cli1.read_response())
    sleep_cnt = 0

    while True:
        cli2.send_command("binlogpos", "0")
        s_pos = int(cli2.read_response())
        assert(s_pos <= m_pos)
        if (s_pos == m_pos):
            print "m/s binlog matches"
            break
        else:
            print "m/s binlog not matches", m_pos, s_pos
            if (sleep_cnt >= 30):
                assert 0, "m/s binlogpos not match"
            time.sleep(1)
            sleep_cnt += 1

    for o in lst:
        cli1.send_command("get", str(i))
        assert(cli1.read_response() == str(i))
        cli2.send_command("get", str(i))
        assert(cli2.read_response() == str(i))
    print "test_repl_match2 pass"

def test_repl_match1(m, s):
    cli1 = redis.Connection(host="127.0.0.1", port=m.port)
    cli2 = redis.Connection(host="127.0.0.1", port=s.port)

    cli2.send_command("slaveof", "127.0.0.1", str(m.port))
    res = cli2.read_response()
    assert(res == "OK")

    for i in xrange(10000):
        cli1.send_command("set", str(i), str(i))
        cli1.read_response()

    cli1.send_command("binlogpos", "0")
    m_pos = int(cli1.read_response())
    sleep_cnt = 0

    while True:
        cli2.send_command("binlogpos", "0")
        s_pos = int(cli2.read_response())
        assert(s_pos <= m_pos)
        if (s_pos == m_pos):
            print "m/s binlog matches"
            break
        else:
            print "m/s binlog not matches", m_pos, s_pos
            if (sleep_cnt >= 10):
                assert 0, "m/s binlogpos not match"
            time.sleep(1)
            sleep_cnt += 1

    for i in xrange(10000):
        cli2.send_command("get", str(i))
        assert(cli2.read_response() == str(i))

def test_repl():
    m = RedisServer(12345, "m_")
    s = RedisServer(12346, "s_")
    m.setup(False)
    s.setup(False)
    test_repl_match1(m, s)

def test_repl1():
    m = RedisServer(12347, "m_")
    s = RedisServer(12348, "s_")
    m.setup(False)
    s.setup(False)
    test_repl_match2(m, s)

if __name__ == '__main__':
    #test_repl()
    test_repl1()
