proc RandomRange { min max } {  
    set rd [expr rand()]  
       
    set result [expr $rd * ($max - $min) + $min]  
      
    return $result  
}  

proc RandomRangeInt { min max } {  
    return [expr int([RandomRange $min $max])]  
}  

start_server {tags {"zscanbyscore"}} { 
    test {INCREX against non existing key} {
        # Create the Sorted Set
        r del zset
        set count 100
        set elements {}
        for {set j 0} {$j < $count} {incr j} {
            lappend elements $j
        }
        for {set j [expr $count - 1]} {$j >= 0} {incr j -1} {
            set index [RandomRangeInt 0 [expr $j + 1]]
            set tempa [lindex $elements $index]
            set tempb [lindex $elements $j]
            set elements [lreplace $elements $index $index $tempb]
            set elements [lreplace $elements $j $j $tempa]
        }

        for {set j 0} {$j < $count} {incr j} {
            set val [lindex $elements $j]
            r zadd zset $j key:$j
        }

        # Test ZSCAN
        set cur -inf
        set keys {}
        while 1 {
            set res [r zscanbyscore zset $cur]
            set cur [lindex $res 0]
            set k [lindex $res 1]
            lappend keys {*}$k
            if {[llength $cur] == 0} break
        }

        set counter 0
        foreach {k v} $keys {
            assert {$k eq "key:$v"}
            assert {$v eq $counter}
            incr counter
        }

        assert_equal [expr $count*2] [llength $keys]
    }
}