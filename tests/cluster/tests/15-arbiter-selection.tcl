# Arbiter selection test

source "../tests/includes/init-tests.tcl"

# Create a cluster with 1 master and 2 slaves and 4 arbiters, so that we have 2
# slaves for master,and set 4 master as arbiter.
# master #0 slaves #1 #2 arbiters #3 #4 #5 #6
test "Create a 5 nodes cluster, and set 4 nodes as arbiter" {
    create_cluster_with_arbiter 1 2 4
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Instance #0 is a master" {
    assert {[RI 0 role] eq {master}}
}

test "Master #0 have 2 connected slaves" {
    assert {[RI 0 connected_slaves] == 2 }
}

test {Slaves of #0 are instance #1 and #2 as expected} {
    set port0 [get_instance_attrib redis 0 port]
    #assert {[lindex [R 5 role] 2] == $port0}
    #assert {[lindex [R 10 role] 2] == $port0}
    wait_for_condition 1000 50 {
            [string match *$port0* [RI 1 master_port]]
    } else {
            fail "Slave of node 1 is not ok"
    }  
    wait_for_condition 1000 50 {
            [string match *$port0* [RI 2 master_port]]
    } else {
            fail "Slave of node 2 is not ok"
    }      
}

test "Instance #1 #2 synced with the master" {
    wait_for_condition 1000 50 {
        [RI 1 master_link_status] eq {up}
    } else {
        fail "Instance #1 master link status is not up"
    }
    wait_for_condition 1000 50 {
        [RI 2 master_link_status] eq {up}
    } else {
        fail "Instance #2 master link status is not up"
    }
}

test "slave can't be arbiter node" {
    catch {R 1 cluster asarbiter} err    
    assert_equal $err {ERR:18,msg:Only master can became arbiter.}
}

set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis 0 port]]

test "only empty node can be arbiter" {
    catch {R 0 cluster asarbiter} err    
    assert_equal $err {ERR:18,msg:To set an arbiter, the node must be empty.}
}

#send importing command from #0 to #4 
test {cluster setslot importing <target_id> <slot>} {
    set source [dict get [get_myself 0] id]
    catch {R 4 cluster setslot importing $source 1} err
    assert_equal $err {ERR:18,msg:Can't importing slots to arbiter node.}
}

#send importing command from #4 to #0
test {cluster setslot <slot> importing <source_id>} {
    set source [dict get [get_myself 4] id]
    catch {R 0 cluster setslot importing $source 1} err
    assert_equal $err {ERR:18,msg:Can't importing slots from arbiter node.}
}

# #3 cluster replicate #4
test {cluster replicate <node_id>} {
    set node [dict get [get_myself 4] id]
    catch {R 3 cluster replicate $node} err
    assert_equal $err {ERR:18,msg:I am an arbiter, can not replicate to others.}
}

# #1 cluster replicate #3
test {cluster replicate <node_id>} {
    set node [dict get [get_myself 3] id]
    catch {R 1 cluster replicate $node} err
    assert_equal $err {ERR:18,msg:can not replicate to an arbiter.}
}

# failover
test "Write data and kill master #0 and make sure that slave #1 switch to be new master" {
    # Write some data the slave can't receive.
    for {set j 0} {$j < 100} {incr j} {
        $cluster set $j $j
    }

    # slave #2 not failover
    R 2 CONFIG SET cluster-slave-no-failover true

    # Kill the master so that a reconnection will not be possible.
    kill_instance redis 0
}

test "Wait for instance #1 (and not #2) to turn into a master" {
    wait_for_condition 1000 50 {
        [RI 1 role] eq {master}
    } else {
        fail "No failover detected"
    }
    R 2 CONFIG SET cluster-slave-no-failover false
}

test "Cluster should eventually be up again" {
    assert_cluster_state ok
}

test "Cluster is writable" {
    cluster_write_test 1
}

test "Node #2 should eventually replicate node #1" {
    set port1 [get_instance_attrib redis 1 port]
    wait_for_condition 1000 50 {
        ([string match *$port1* [RI 2 master_port]]) &&
        ([RI 2 master_link_status] eq {up})
    } else {
        fail "#2 didn't became slave of #1"
    }
}

# manual failover force
test "Make instance #1 unreachable without killing it" {
   R 1 deferred 1
   R 1 tendisadmin sleep 10
   after 2000
}

test "Send CLUSTER FAILOVER to instance #2" {
    R 2 cluster failover force
}

test "Instance #2 is a master after some time" {
    wait_for_condition 1000 50 {
        [RI 2 role] eq {master}
    } else {
        fail "Instance #2 is not a master after some time regardless of FORCE"
    }
}

test "Wait for instance #1 to return back alive" {
    R 1 deferred 0
    assert {[R 1 read] eq {OK}}
}

test "Cluster should eventually be up again" {
    assert_cluster_state ok
}

test "Cluster is writable" {
    cluster_write_test 2
}