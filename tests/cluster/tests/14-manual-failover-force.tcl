## Check that manual failover does not happen if we can't talk with the master.

source "../tests/includes/init-tests.tcl"

test "Create a 5 nodes cluster" {
    create_cluster 5 5
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Cluster is writable" {
    cluster_write_test 0
}

test "Instance #5 is a slave" {
    assert {[RI 5 role] eq {slave}}
}

test "Instance #5 synced with the master" {
    wait_for_condition 1000 50 {
        [RI 5 master_link_status] eq {up}
    } else {
        fail "Instance #5 master link status is not up"
    }
}

test "Make instance #0 unreachable without killing it" {
   R 0 deferred 1
   # R 0 DEBUG SLEEP 10
   R 0 tendisadmin sleep 10
}

test "Send CLUSTER FAILOVER to instance #5" {
    R 5 cluster failover
}

test "Instance #5 is still a slave after some time (no failover)" {
    after 5500
    assert {[RI 5 role] eq {slave}}
}



test "Wait for instance #0 to return back alive" {
    after 5000
    R 0 deferred 0
    assert {[R 0 read] eq {OK}}
}


