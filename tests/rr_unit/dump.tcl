start_server {tags {"dump"}} {
    test {DUMP / RESTORE are able to serialize / unserialize a simple key} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        list [r exists foo] [r restore foo 0 $encoded] [r ttl foo] [r get foo]
    } {0 OK -1 bar}

    test {DUMP / RESTORE are able to serialize / unserialize a simple list} {
        r del foo
        r rpush foo bar 1
        set encoded [r dump foo]
        r del foo
        list [r exists foo] [r restore foo 0 $encoded] [r ttl foo] [r lrange foo 0 -1]
    } {0 OK -1 {bar 1}}

    test {DUMP / RESTORE are able to serialize / unserialize a simple hash} {
        r del foo
        r hmset foo 1 2 3 4 5 6
        set encoded [r dump foo]
        r del foo
        list [r exists foo] [r restore foo 0 $encoded] [r ttl foo] [r hgetall foo]
    } {0 OK -1 {1 2 3 4 5 6}}

    test {DUMP / RESTORE are able to serialize / unserialize a simple set} {
        r del foo
        r sadd foo 1 2 3 4 5 6
        set encoded [r dump foo]
        r del foo
        list [r exists foo] [r restore foo 0 $encoded] [r ttl foo] [r smembers foo]
    } {0 OK -1 {1 2 3 4 5 6}}

    test {DUMP / RESTORE are able to serialize / unserialize a simple zset} {
        r del foo
        r zadd foo 1 1 1 2 1 3 1 4 1 5 1 6aaaaa
        set encoded [r dump foo]
        r del foo
        list [r exists foo] [r restore foo 0 $encoded] [r ttl foo] [r zrange foo 0 -1 withscores]
    } {0 OK -1 {1 1 2 1 3 1 4 1 5 1 6aaaaa 1}}

    test {RESTORE can set an arbitrary expire to the materialized key} {
        r del foo
        r set foo bar
        set encoded [r dump foo]
        r del foo
        r restore foo 5000 $encoded
        set ttl [r pttl foo]
        assert {$ttl >= 3000 && $ttl <= 5000}
        r get foo
    } {bar}

    test {RESTORE can set an expire that overflows a 32 bit integer} {
        r del foo
        r set foo bar
        set encoded [r dump foo]
        r del foo
        r restore foo 2569591501 $encoded
        set ttl [r pttl foo]
        assert {$ttl >= (2569591501-3000) && $ttl <= 2569591501}
        r get foo
    } {bar}

    test {RESTORE returns an error of the key already exists} {
        r set foo bar
        set encoded [r dump foo]
        set e {}
        catch {r restore foo 0 $encoded} e
        set e
    } {*is busy*}

    test {DUMP of non existing key returns nil} {
        r dump nonexisting_key
    } {}

    test {MIGRATE is able to migrate a key between two instances} {
        set first [srv 0 client]
        r set key "Some Value"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key] == 1}
            assert {[$second exists key] == 0}
            set ret [r -1 migrate $second_host $second_port key 9 5000]
            assert {$ret eq {OK}}
            assert {[$first exists key] == 0}
            assert {[$second exists key] == 1}
            assert {[$second get key] eq {Some Value}}
            assert {[$second ttl key] == -1}
        }
    }

    test {MIGRATE propagates TTL correctly} {
        set first [srv 0 client]
        r set key "Some Value"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key] == 1}
            assert {[$second exists key] == 0}
            $first expire key 10
            set ret [r -1 migrate $second_host $second_port key 9 5000]
            assert {$ret eq {OK}}
            assert {[$first exists key] == 0}
            assert {[$second exists key] == 1}
            assert {[$second get key] eq {Some Value}}
            assert {[$second ttl key] >= 7 && [$second ttl key] <= 10}
        }
    }

    test {MIGRATE can correctly transfer large values} {
        set first [srv 0 client]
        r del key
        for {set j 0} {$j < 5000} {incr j} {
            r rpush key 1 2 3 4 5 6 7 8 9 10
            r rpush key "item 1" "item 2" "item 3" "item 4" "item 5" \
                        "item 6" "item 7" "item 8" "item 9" "item 10"
        }
        assert {[string length [r dump key]] > (1024*64)}
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key] == 1}
            assert {[$second exists key] == 0}
            set ret [r -1 migrate $second_host $second_port key 9 100000]
            assert {$ret eq {OK}}
            assert {[$first exists key] == 0}
            assert {[$second exists key] == 1}
            assert {[$second ttl key] == -1}
            assert {[$second llen key] == 5000*20}
        }
    }

    test {MIGRATE can correctly transfer hashes} {
        set first [srv 0 client]
        r del key
        r hmset key field1 "item 1" field2 "item 2" field3 "item 3" \
                    field4 "item 4" field5 "item 5" field6 "item 6"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key] == 1}
            assert {[$second exists key] == 0}
            set ret [r -1 migrate $second_host $second_port key 9 10000]
            assert {$ret eq {OK}}
            assert {[$first exists key] == 0}
            assert {[$second exists key] == 1}
            assert {[$second ttl key] == -1}
        }
    }

    #test {MIGRATE can correctly transfer set} {
    #    set first [srv 0 client]
    #    r del key
    #    r sadd key field1 "item 1" field2 "item 2" field3 "item 3" \
    #                field4 "item 4" field5 "item 5" field6 "item 6"
    #    start_server {tags {"repl"}} {
    #        set second [srv 0 client]
    #        set second_host [srv 0 host]
    #        set second_port [srv 0 port]

    #        assert {[$first exists key] == 1}
    #        assert {[$second exists key] == 0}
    #        set ret [r -1 migrate $second_host $second_port key 9 10000]
    #        assert {$ret eq {OK}}
    #        assert {[$first exists key] == 0}
    #        assert {[$second exists key] == 1}
    #        assert {[$second ttl key] == -1}
    #    }
    #}

    test {MIGRATE can correctly transfer zset} {
        set first [srv 0 client]
        r del key
        r zadd key 1 "item 1" 2 "item 2" 3 "item 3" 6 "item 4" 5 "item 5" 4 "item 6"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key] == 1}
            assert {[$second exists key] == 0}
            set ret [r -1 migrate $second_host $second_port key 9 10000]
            assert {$ret eq {OK}}
            assert {[$first exists key] == 0}
            assert {[$second exists key] == 1}
            assert {[$second ttl key] == -1}
        }
    }

    test {MIGRATE timeout actually works} {
        set first [srv 0 client]
        r set key "Some Value"
        start_server {tags {"repl"}} {
            set second [srv 0 client]
            set second_host [srv 0 host]
            set second_port [srv 0 port]

            assert {[$first exists key] == 1}
            assert {[$second exists key] == 0}

            set rd [redis_deferring_client]
            $rd debug sleep 5.0 ; # Make second server unable to reply.
            set e {}
            catch {r -1 migrate $second_host $second_port key 9 1000} e
            assert_match {IOERR*} $e
        }
    }
}
