start_server {tags {"repl"}} {
    if {$::accurate} {set numops 800000} else {set numops 120000}
    createComplexDataset r $numops

    start_server {} {
        test {MASTER and SLAVE consistency with resume paste} {
            r slaveof [srv -1 host] [srv -1 port]
            wait_sync 7200000 3600000 120000 120000
            
            r slaveof no one
            wait_stop 30000 10000
            
            # Wait until sync finsihed
            r slaveof [srv -1 host] [srv -1 port]
            set handle [start_bg_complex_data [srv -1 host] [srv -1 port] 9 $numops]
            wait_sync 7200000 3600000 3600000 300000
            stop_bg_complex_data $handle 

            r slaveof no one
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
