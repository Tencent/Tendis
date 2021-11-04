source "../tests/includes/init-tests.tcl"

# Create a cluster with 3 master and 15 slaves, so that we have 5
# slaves for eatch master.
test "Create a 3 nodes cluster" {
    create_cluster 3 15
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "The first master has actually 5 slaves" {
    assert {[llength [lindex [R 0 role] 2]] == 5}
}

test {Slaves of #0 are instance #3, #6, #9, #12 and #15 as expected} {
    set port0 [get_instance_attrib redis 0 port]
    assert {[lindex [R 3 role] 2] == $port0}
    assert {[lindex [R 6 role] 2] == $port0}
    assert {[lindex [R 9 role] 2] == $port0}
    assert {[lindex [R 12 role] 2] == $port0}
    assert {[lindex [R 15 role] 2] == $port0}
}

test {Instance #3, #6, #9, #12 and #15 synced with the master} {
    wait_for_condition 1000 50 {
        [RI 3 master_link_status] eq {up} &&
        [RI 6 master_link_status] eq {up} &&
        [RI 9 master_link_status] eq {up} &&
        [RI 12 master_link_status] eq {up} &&
        [RI 15 master_link_status] eq {up}
    } else {
        fail "Instance #3 or #6 or #9 or #12 or #15 master link status is not up"
    }
}

proc master_detected {instances} {
    foreach instance [dict keys $instances] {
        if {[RI $instance role] eq {master}} {
            return true
        }
    }

    return false
}

test "New Master down consecutively" {
    set instances [dict create 0 1 3 1 6 1 9 1 12 1 15 1]

    set loops [expr {[dict size $instances]-1}]
    for {set i 0} {$i < $loops} {incr i} {
        set master_id -1
        foreach instance [dict keys $instances] {
            if {[RI $instance role] eq {master}} {
                set master_id $instance
                break;
            }
        }

        if {$master_id eq -1} {
            fail "no master detected, #loop $i"
        }

        after 35000
        set instances [dict remove $instances $master_id]

        kill_instance redis $master_id
        wait_for_condition 1000 50 {
            [master_detected $instances]
        } else {
            fail "No failover detected when master $master_id fails"
        }

        assert_cluster_state ok
    }
}
