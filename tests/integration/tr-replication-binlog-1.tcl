start_server {tags {"repl"} redis2817} {
    if {$::accurate} {set numops 800000} else {set numops 120000}
    createComplexDataset r $numops {redis2817 useexpire}

    start_server {} {
        r config set repl-mode redis-rdbdump
        r -1 config set repl-timeout 6000
        r slaveof [srv -1 host] [srv -1 port]

        wait_stop 2000000 60000

        start_server {} {
            test {(not empty) MASTER <=(rdbdump)=> SLAVE <=(binlog)=> SLAVE consistency} {
                r -1 config set log-count 20000000
                r -1 config set log-keep-count 20000000

                r slaveof [srv -1 host] [srv -1 port]
                set handle [start_bg_complex_data [srv -2 host] [srv -2 port] 9 $numops]
                wait_sync 7200000 7200000 2000000 60000

                r -1 slaveof no one
                r slaveof no one

                r debug reload
                r -1 debug reload

                set digest1 [r debug digest]
                set digest2 [r -1 debug digest]

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
                comparedb {r -1} {r -2}
            }
        }
    }
}
