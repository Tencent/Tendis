start_server {tags {"repl"}} {
    start_server {} {
        test {First server should have role slave after SLAVEOF} {
            r -1 slaveof [srv 0 host] [srv 0 port]
            wait_for_condition 50 100 {
                [s -1 master_link_status] eq {up}
            } else {
                fail "Replication not started."
            }
        }

        if {$::accurate} {set numops 800000} else {set numops 120000}

        test {MASTER and SLAVE consistency follow up} {
            createComplexDataset r $numops {tredis useexpire}
            wait_sync 7200000 3600000 900000 300000

            r -1 slaveof no one
            wait_stop 30000 10000

            r debug reload
            r -1 debug reload
            
            set digest1 [r debug digest]
            set digest2 [r debug digest]
            if {$digest1 ne $digest2} {
                set csv1 [csvdump r]
                set csv2 [csvdump {r -1}]
                set fd [open /tmp/repldump1.txt w]
                puts -nonewline $fd $csv1
                close $fd
                set fd [open /tmp/repldump2.txt w]
                puts -nonewline $fd $csv2
                close $fd
                puts "Master - Slave inconsistency"
                puts "Run diff -u against /tmp/repldump*.txt for more info"
            }
            assert_equal $digest1 $digest2
        }
    }
}
