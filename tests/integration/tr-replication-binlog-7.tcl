start_server {tags {"repl"} redis2817} {
    if {$::accurate} {set numops 1000000} else {set numops 500000}
    if {$::valgrind} {set numops 120000}

    createComplexDataset r $numops {redis2817 useexpire}

    start_server {} {
        r config set repl-timeout 3000
        r config set repl-mode redis-rdbdump

        r -1 config set repl-timeout 3000
        r slaveof [srv -1 host] [srv -1 port]
        wait_stop 180000 30000 

        start_server {} {
            test {(not empty) MASTER <-*-(rdb-dump)-*-> SLAVE <-*-(binlog)-*-> SLAVE consistency} {
                r slaveof [srv -1 host] [srv -1 port]

                set handle0 [start_bg_complex_data [srv -2 host] [srv -2 port] 9 $numops]
                wait_sync 7200000 3600000 1200000 60000
                stop_bg_complex_data $handle0

                r -1 slaveof no one
                set handle1 [start_bg_complex_data [srv -2 host] [srv -2 port] 9 $numops]
                r -1 slaveof [srv -2 host] [srv -2 port]
                wait_sync 7200000 3600000 5400000 2400000
                stop_bg_complex_data $handle1

                r slaveof no one
                r -1 slaveof no one
                wait_stop 30000 10000

                r debug reload
                r -1 debug reload

                set debug_digest1 [r debug digest]
                set debug_digest2 [r -1 debug digest]

                assert_equal $debug_digest1 $debug_digest2
                comparedb {r -1} {r -2}
            }
        }
    }
}
