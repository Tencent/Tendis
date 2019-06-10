start_server {tags {"lazyfree"}} {
    test "UNLINK can reclaim memory in background" {
        # set orig_mem [s used_memory]
        set args {}
        for {set i 0} {$i < 50000} {incr i} {
            lappend args $i
        }
        r sadd myset {*}$args
        assert {[r scard myset] == 50000}
        # set peak_mem [s used_memory]
        assert {[r unlink myset] == 1}

        # assert {$peak_mem > $orig_mem+100000}
        # wait_for_condition 50 100 {
        #     [s used_memory] < $peak_mem &&
        #     [s used_memory] < $orig_mem*2
        # } else {
        #     fail "Memory is not reclaimed by UNLINK"
        # }
    }
}
