start_server {tags {"repl"}} {
    start_server {} {
        test {First server should have role slave after SLAVEOF} {
            r -1 slaveof [srv 0 host] [srv 0 port]
            wait_for_condition 50 100 {
                [s -1 role] eq {slave} &&
                [string match {*master_link_status:up*} [r -1 info replication]]
            } else {
                fail "Can't turn the instance into a slave"
            }
        }

        test {BRPOPLPUSH replication, when blocking against empty list} {
            set rd [redis_deferring_client]
            $rd brpoplpush a b 5
            r lpush a foo
            r stopclean
            after 10000
            wait_for_condition 50 100 {
                [r debug digest] eq [r -1 debug digest]
            } else {
                fail "Master and slave have different digest: [r debug digest] VS [r -1 debug digest]"
            }
        }

        test {BRPOPLPUSH replication, list exists} {
            set rd [redis_deferring_client]
            r lpush c 1
            r lpush c 2
            r lpush c 3
            $rd brpoplpush c d 5
            after 1000
            r stopclean
            after 10000
            set digest1 [r debug digest]
            set digest2 [r debug digest]
            assert_equal $digest1 $digest2
        }
    }
}

start_server {tags {"repl"}} {
    r set mykey foo

    start_server {} {
        test {Second server should have role master at first} {
            s role
        } {master}

        test {SLAVEOF should start with link status "down"} {
            r slaveof [srv -1 host] [srv -1 port]
            s master_link_status
        } {down}

        test {The role should immediately be changed to "slave"} {
            s role
        } {slave}

        after 2000
        wait_for_sync r
        test {Sync should have transferred keys from master} {
            r get mykey
        } {foo}

        test {The link status should be up} {
            s master_link_status
        } {up}

        test {SET on the master should immediately propagate} {
            r -1 set mykey bar

            wait_for_condition 500 100 {
                [r 0 get mykey] eq {bar}
            } else {
                fail "SET on master did not propagated on slave"
            }
        }

        test {MSET on the master should immediately propagate} {
            r -1 mset msetmykey bar
            r -1 expire msetmykey 1000

            wait_for_condition 500 100 {
                [r 0 get msetmykey] eq {bar}
            } else {
                fail "MSET on master did not propagated on slave"
            }

            assert {[r 0 ttl msetmykey] > 500 && [r 0 ttl msetmykey] <= 1000}
        }

        test {FLUSHALL should replicate} {
            r -1 flushalldisk
            if {$::valgrind} {after 2000}
            list [r -1 dbsize] [r 0 dbsize]
        } {0 0}

        test {FLUSHALL should replicate and OK} {
            r -1 flushalldisk
            after 30000
            list [r -1 keys *] [r 0 keys *]
        } {{} {}}

        test {HSET on the master should immediately propagate} {
            r -1 hset myhash hk1 bar
            r -1 hset myhash hk2 bar2
            r -1 hmset myhash hk3 bar3 hk4 bar4

            wait_for_condition 500 100 {
                [r 0 hget myhash hk1] eq {bar}
            } else {
                fail "HSET hk1 on master did not propagated on slave"
            }
            wait_for_condition 500 100 {
                [r 0 hget myhash hk2] eq {bar2}
            } else {
                fail "HSET hk2 on master did not propagated on slave"
            }
            wait_for_condition 500 100 {
                [r 0 hget myhash hk3] eq {bar3}
            } else {
                fail "HMSET hk3 on master did not propagated on slave"
            }
            wait_for_condition 500 100 {
                [r 0 hget myhash hk4] eq {bar4}
            } else {
                fail "HMSET hk4 on master did not propagated on slave"
            }
        }

        test {lpush on the master should immediately propagate} {
            r -1 lpush mylist item1
            r -1 lpush mylist item2

            wait_for_condition 500 100 {
                [r 0 lindex mylist 0] eq {item2}
            } else {
                fail "lpush on master did not propagated on slave"
            }
            r -1 lpush mylist item3 item4
            wait_for_condition 500 100 {
                [r 0 lindex mylist 0] eq {item4}
            } else {
                fail "lpush on master did not propagated on slave"
            }
          }

        test {ZADD on the master should immediately propagate} {
            r -1 zadd myzset 1 item1
            r -1 zadd myzset 2 item2

            wait_for_condition 500 100 {
                [r 0 zrange myzset 0 0] eq {item1}
            } else {
                fail "zadd on master did not propagated on slave"
            }

            wait_for_condition 500 100 {
                [r 0 zrevrange myzset 0 0] eq {item2}
            } else {
                fail "zadd on master did not propagated on slave"
            }
          }

        test {ROLE in master reports master with a slave} {
            set res [r -1 role]
            lassign $res role offset slaves
            assert {$role eq {master}}
            assert {$offset > 0}
            assert {[llength $slaves] == 1}
            lassign [lindex $slaves 0] master_host master_port slave_offset
            assert {$slave_offset <= $offset}
        }

        test {ROLE in slave reports slave in connected state} {
            after 30000
            set res [r role]
            set res2 [r info all]
            lassign $res role master_host master_port slave_state slave_offset
            assert {$role eq {slave}}
            assert {$slave_state eq {connected}}
        }
    }
}

start_server {tags {"repl"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set slaves {}
    set load_handle0 [start_write_load $master_host $master_port 3]
    set load_handle1 [start_write_load $master_host $master_port 5]
    set load_handle2 [start_write_load $master_host $master_port 20]
    set load_handle3 [start_write_load $master_host $master_port 8]
    set load_handle4 [start_write_load $master_host $master_port 4]
    start_server {} {
        lappend slaves [srv 0 client]
        start_server {} {
            lappend slaves [srv 0 client]
            start_server {} {
                lappend slaves [srv 0 client]
                test "Connect multiple slaves at the same time (issue #141)" {
                    # Send SALVEOF commands to slaves
                    [lindex $slaves 0] slaveof $master_host $master_port
                    [lindex $slaves 1] slaveof $master_host $master_port
                    [lindex $slaves 2] slaveof $master_host $master_port

                    # TODO: fixme
                    # Wait for all the three slaves to reach the "online" state
                    #set retry 500
                    #while {$retry} {
                    #    set info [r -3 info]
                    #    if {[string match {*slave0:*state=online*slave1:*state=online*slave2:*state=online*} $info]} {
                    #        break
                    #    } else {
                    #        incr retry -1
                    #        after 100
                    #    }
                    #}
                    #if {$retry == 0} {
                    #    error "assertion:Slaves not correctly synchronized"
                    #}

                    # Stop the write load
                    stop_write_load $load_handle0
                    stop_write_load $load_handle1
                    stop_write_load $load_handle2
                    stop_write_load $load_handle3
                    stop_write_load $load_handle4

                    # Wait that slaves exit the "loading" state
                    wait_for_condition 500 100 {
                        ![string match {*loading:1*} [[lindex $slaves 0] info]] &&
                        ![string match {*loading:1*} [[lindex $slaves 1] info]] &&
                        ![string match {*loading:1*} [[lindex $slaves 2] info]]
                    } else {
                        fail "Slaves still loading data after too much time"
                    }

                    # Make sure that slaves and master have same number of keys
                    #wait_for_condition 500 100 {
                    #    [$master dbsize] == [[lindex $slaves 0] dbsize] &&
                    #    [$master dbsize] == [[lindex $slaves 1] dbsize] &&
                    #    [$master dbsize] == [[lindex $slaves 2] dbsize]
                    #} else {
                    #    fail "Different number of keys between master and slave after too long time."
                    #}

                    after 30000
                    r stopclean
                    [lindex $slaves 0] stopclean
                    [lindex $slaves 1] stopclean
                    [lindex $slaves 2] stopclean

                    after 30000

                    # Check digests
                    set digest [$master debug digest]
                    set digest0 [[lindex $slaves 0] debug digest]
                    set digest1 [[lindex $slaves 1] debug digest]
                    set digest2 [[lindex $slaves 2] debug digest]

                    assert {$digest ne 0000000000000000000000000000000000000000}
                    assert {$digest eq $digest0}
                    assert {$digest eq $digest1}
                    assert {$digest eq $digest2}
                }
           }
        }
    }
}
