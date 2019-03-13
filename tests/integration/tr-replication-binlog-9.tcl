start_server {tags {"repl"}} {
    if {$::accurate} {set numops 1000000} else {set numops 500000}
    if {$::valgrind} {set numops 120000}

    createComplexDataset r $numops
    
    start_server {} {
      test { test flushalldisk sync to slave } {
        set handle [start_bg_complex_data [srv -1 host] [srv -1 port] 9 $numops]
        wait_stop 180000 30000 
        stop_bg_complex_data $handle

        r slaveof [srv -1 host] [srv -1 port]
        r -1 flushalldisk

        wait_stop 30000 10000
        r -1 slaveof no one

        r debug reload
        r -1 debug reload

        set debug_digest1 [r debug digest]
        set debug_digest2 [r -1 debug digest]

        assert_equal $debug_digest1 $debug_digest2 
  }
  }
}
