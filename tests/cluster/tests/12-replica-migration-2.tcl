# Replica migration test #2.
#
# Check that the status of master that can be targeted by replica migration
# is acquired again, after being getting slots again, in a cluster where the
# other masters have slaves.

source "../tests/includes/init-tests.tcl"

# Create a cluster with 5 master and 15 slaves, to make sure there are no
# empty masters and make rebalancing simpler to handle during the test.
test "Create a 5 nodes cluster" {
    create_cluster 5 15
}

test "Cluster is up" {
    assert_cluster_state ok
}

test "Each master should have at least two replicas attached" {
    foreach_redis_id id {
        if {$id < 5} {
            wait_for_condition 1000 50 {
                [RI $id connected_slaves] >= 2
            } else {
                fail "Master #$id does not have 2 slaves as expected"
            }
        }
    }
}

test "Resharding all the master #0 slots away from it" {
    set output [exec ../migrate.sh >@ stdout]
    after 10000
}

test "Master #0 should lose its replicas" {
    wait_for_condition 1000 50 {
        [RI 0 connected_slaves] == 0
    } else {
        fail "Master #0 still has replicas"
    }
}

test "Resharding back some slot to master #0" {
    # Wait for the cluster config to propagate before attempting a
    # new resharding.
    after 30000
    set output [exec ../migrate-back.sh >@ stdout]
}

test "Master #0 should re-acquire one or more replicas" {
    wait_for_condition 1000 50 {
        [RI 0 connected_slaves] >= 1
    } else {
        fail "Master #0 has no has replicas"
    }
}
