start_server {tags {"tbitops"}} {
    #tsetbit
    test "tsetbit not exists key" {
        r del key
        assert_equal "0" [r tsetbit key 0 1]
        assert_equal "0" [r tsetbit key 8192 1]
    }

    test "tsetbit exists key" {
        assert_equal "1" [r tsetbit key 0 1]
        assert_equal "1" [r tsetbit key 8192 1]
    }

    test "tsetbit wrong pos" {
        r del key
        assert_error "*bit offset is not an integer or out of range*" {r tsetbit key 999999999999999 1}
        assert_error "*bit offset is not an integer or out of range*" {r tsetbit key -1 1}
    }

    test "tsetbit wrong bit" {
        r del key
        assert_error "*bit offset is not an integer or out of range*" {r tsetbit key 999999999999999 2}
        assert_error "*bit is not an integer or out of range*" {r tsetbit key 0 2}
        assert_error "*bit is not an integer or out of range*" {r tsetbit key 0 -1}
    }

    # tgetbit
    test "tgetbit not exists key" {
        r del key
        assert_equal "0" [r tgetbit key 0]
        assert_equal "0" [r tgetbit key 8192]
    }

    test "tgetbit exists key" {
        r del key
        assert_equal "0" [r tsetbit key 0 1]
        assert_equal "1" [r tgetbit key 0]
        assert_equal "0" [r tsetbit key 8192 1]
        assert_equal "1" [r tgetbit key 8192]
    }

    test "tgetbit wrong pos" {
        r del key
        assert_error "*bit offset is not an integer or out of range*" {r tgetbit key 999999999999999}
        assert_error "*bit offset is not an integer or out of range*" {r tgetbit key -1}
    }

    # tbitcount
    test "tbitcount all" {
        r del key
        assert_equal "0" [r tbitcount key]
        assert_equal "0" [r tsetbit key 0 1]
        assert_equal "1" [r tbitcount key]
        assert_equal "1" [r tsetbit key 0 0]
        assert_equal "0" [r tbitcount key]
        assert_equal "0" [r tsetbit key 8192 1]
        assert_equal "1" [r tbitcount key]
        assert_equal "1" [r tsetbit key 8192 0]
        assert_equal "0" [r tbitcount key]
    }

    test "tbitcount range" {
        r del key
        assert_equal "0" [r tsetbit key 1024 1]
        assert_equal "0" [r tsetbit key 2048 1]
        assert_equal "0" [r tsetbit key 4096 1]

        assert_equal "3" [r tbitcount key]
        assert_equal "3" [r tbitcount key 128]
        assert_equal "2" [r tbitcount key 256]
        assert_equal "1" [r tbitcount key 512]
        assert_equal "0" [r tbitcount key 513]

        assert_equal "0" [r tbitcount key 0 127]
        assert_equal "1" [r tbitcount key 0 128]
        assert_equal "2" [r tbitcount key 0 256]
        assert_equal "3" [r tbitcount key 0 512]
    }

    test "tbitcount cross" {
        r del key
        assert_equal "0" [r tsetbit key 1024 1]
        assert_equal "0" [r tsetbit key 2048 1]
        assert_equal "0" [r tsetbit key 4096 1]

        assert_equal "0" [r tsetbit key 8192 1]
        assert_equal "0" [r tsetbit key 9126 1]
        assert_equal "0" [r tsetbit key 10240 1]
        
        assert_equal "6" [r tbitcount key]
        assert_equal "3" [r tbitcount key 512 1140]
    }

    test "tbitcount negative pos" {
        r del key
        assert_equal "0" [r tsetbit key 1024 1]
        assert_equal "0" [r tsetbit key 2048 1]
        assert_equal "0" [r tsetbit key 4096 1]

        assert_equal "3" [r tbitcount key -512 -1]
        assert_equal "0" [r tbitcount key -1024 0]
        assert_equal "0" [r tbitcount key -512 -1024]
        assert_equal "0" [r tbitcount key 1024 512]
    }

    # tbitpos
    test "tbitpos not exists key" {
        r del key
        assert_equal "0" [r tbitpos key 0]
        assert_equal "-1" [r tbitpos key 1]
        assert_equal "0" [r tbitpos key 0 100 200]
        assert_equal "-1" [r tbitpos key 1 100 200]
    }

    test "tbitpos exists key" {
        r del key
        assert_equal "0" [r tsetbit key 2048 1]
        assert_equal "0" [r tsetbit key 4096 1]
        assert_equal "0" [r tsetbit key 8192 1]
        assert_equal "0" [r tsetbit key 9126 1]

        assert_equal "2048" [r tbitpos key 1]
        assert_equal "2048" [r tbitpos key 1 256]
        assert_equal "4096" [r tbitpos key 1 257]
        assert_equal "8192" [r tbitpos key 1 1024]
        assert_equal "9126" [r tbitpos key 1 1025]

        assert_equal "2048" [r tbitpos key 1 0 1024]
    }

    test "tbitpos wrong key" {
        r del key
        assert_equal "0" [r tsetbit key 1024 1]
        assert_equal "0" [r tsetbit key 2048 1]
        assert_equal "0" [r tsetbit key 4096 1]

        assert_equal "-1" [r tbitpos key 1 999999999999999]
        assert_equal "-1" [r tbitpos key 0 999999999999999]  
        assert_equal "4097" [r tbitpos key 0 -1] 
        assert_equal "4096" [r tbitpos key 1 -1]
    }

    test "tbitpos wrong bit" {
        r del key
        assert_error "*The bit argument must be 1 or 0.*" {r tbitpos key 2}
        assert_error "*The bit argument must be 1 or 0.*" {r tbitpos key -1}
    }

    # bitfield
    test {tbitfield signed SET and GET basics} {
        r del bits
        set results {}
        lappend results [r tbitfield bits set i8 0 -100]
        lappend results [r tbitfield bits set i8 0 101]
        lappend results [r tbitfield bits get i8 0]
        set results
    } {0 -100 101}

    test {tbitfield unsigned SET and GET basics} {
        r del bits
        set results {}
        lappend results [r tbitfield bits set u8 0 255]
        lappend results [r tbitfield bits set u8 0 100]
        lappend results [r tbitfield bits get u8 0]
        set results
    } {0 255 100}

    test {tbitfield basic INCRBY form} {
        r del bits
        set results {}
        r tbitfield bits set u8 #0 10
        lappend results [r tbitfield bits incrby u8 #0 100]
        lappend results [r tbitfield bits incrby u8 #0 100]
        set results
    } {110 210}

    test {tbitfield chaining of multiple commands} {
        r del bits
        set results {}
        r tbitfield bits set u8 #0 10
        lappend results [r tbitfield bits incrby u8 #0 100 incrby u8 #0 100]
        set results
    } {{110 210}}

    test {tbitfield unsigned overflow wrap} {
        r del bits
        set results {}
        r tbitfield bits set u8 #0 100
        lappend results [r tbitfield bits overflow wrap incrby u8 #0 257]
        lappend results [r tbitfield bits get u8 #0]
        lappend results [r tbitfield bits overflow wrap incrby u8 #0 255]
        lappend results [r tbitfield bits get u8 #0]
    } {101 101 100 100}

    test {tbitfield unsigned overflow sat} {
        r del bits
        set results {}
        r tbitfield bits set u8 #0 100
        lappend results [r tbitfield bits overflow sat incrby u8 #0 257]
        lappend results [r tbitfield bits get u8 #0]
        lappend results [r tbitfield bits overflow sat incrby u8 #0 -255]
        lappend results [r tbitfield bits get u8 #0]
    } {255 255 0 0}

    test {tbitfield signed overflow wrap} {
        r del bits
        set results {}
        r tbitfield bits set i8 #0 100
        lappend results [r tbitfield bits overflow wrap incrby i8 #0 257]
        lappend results [r tbitfield bits get i8 #0]
        lappend results [r tbitfield bits overflow wrap incrby i8 #0 255]
        lappend results [r tbitfield bits get i8 #0]
    } {101 101 100 100}

    test {tbitfield signed overflow sat} {
        r del bits
        set results {}
        r tbitfield bits set u8 #0 100
        lappend results [r tbitfield bits overflow sat incrby i8 #0 257]
        lappend results [r tbitfield bits get i8 #0]
        lappend results [r tbitfield bits overflow sat incrby i8 #0 -255]
        lappend results [r tbitfield bits get i8 #0]
    } {127 127 -128 -128}

    test {tbitfield overflow detection fuzzing} {
        for {set j 0} {$j < 1000} {incr j} {
            set bits [expr {[randomInt 64]+1}]
            set sign [randomInt 2]
            set range [expr {2**$bits}]
            if {$bits == 64} {set sign 1} ; # u64 is not supported by tbitfield.
            if {$sign} {
                set min [expr {-($range/2)}]
                set type "i$bits"
            } else {
                set min 0
                set type "u$bits"
            }
            set max [expr {$min+$range-1}]

            # Compare Tcl vs Redis
            set range2 [expr {$range*2}]
            set value [expr {($min*2)+[randomInt $range2]}]
            set increment [expr {($min*2)+[randomInt $range2]}]
            if {$value > 9223372036854775807} {
                set value 9223372036854775807
            }
            if {$value < -9223372036854775808} {
                set value -9223372036854775808
            }
            if {$increment > 9223372036854775807} {
                set increment 9223372036854775807
            }
            if {$increment < -9223372036854775808} {
                set increment -9223372036854775808
            }

            set overflow 0
            if {$value > $max || $value < $min} {set overflow 1}
            if {($value + $increment) > $max} {set overflow 1}
            if {($value + $increment) < $min} {set overflow 1}

            r del bits
            set res1 [r tbitfield bits overflow fail set $type 0 $value]
            set res2 [r tbitfield bits overflow fail incrby $type 0 $increment]

            if {$overflow && [lindex $res1 0] ne {} &&
                             [lindex $res2 0] ne {}} {
                fail "OW not detected where needed: $type $value+$increment"
            }
            if {!$overflow && ([lindex $res1 0] eq {} ||
                               [lindex $res2 0] eq {})} {
                fail "OW detected where NOT needed: $type $value+$increment"
            }
        }
    }

    test {tbitfield overflow wrap fuzzing} {
        for {set j 0} {$j < 1000} {incr j} {
            set bits [expr {[randomInt 64]+1}]
            set sign [randomInt 2]
            set range [expr {2**$bits}]
            if {$bits == 64} {set sign 1} ;
            if {$sign} {
                set min [expr {-($range/2)}]
                set type "i$bits"
            } else {
                set min 0
                set type "u$bits"
            }
            set max [expr {$min+$range-1}]

            # Compare Tcl vs Redis
            set range2 [expr {$range*2}]
            set value [expr {($min*2)+[randomInt $range2]}]
            set increment [expr {($min*2)+[randomInt $range2]}]
            if {$value > 9223372036854775807} {
                set value 9223372036854775807
            }
            if {$value < -9223372036854775808} {
                set value -9223372036854775808
            }
            if {$increment > 9223372036854775807} {
                set increment 9223372036854775807
            }
            if {$increment < -9223372036854775808} {
                set increment -9223372036854775808
            }

            r del bits
            r tbitfield bits overflow wrap set $type 0 $value
            r tbitfield bits overflow wrap incrby $type 0 $increment
            set res [lindex [r tbitfield bits get $type 0] 0]

            set expected 0
            if {$sign} {incr expected [expr {$max+1}]}
            incr expected $value
            incr expected $increment
            set expected [expr {$expected % $range}]
            if {$sign} {incr expected $min}

            if {$res != $expected} {
                fail "WRAP error: $type $value+$increment = $res, should be $expected"
            }
        }
    }

    test {tbitfield regression for #3564} {
        for {set j 0} {$j < 10} {incr j} {
            r del mystring
            set res [r tbitfield mystring SET i8 0 10 SET i8 64 10 INCRBY i8 10 99900]
            assert {$res eq {0 0 60}}
        }
        r del mystring
    }

    test {tbitfield update meta} {
        r del key
        assert_equal "0" [r tsetbit key 0 1]
        assert_equal "1" [r tbitcount key]
        assert_equal "0" [r tbitfield key set u1 1 1]
        assert_equal "1" [r tbitfield key incrby u1 2 1]
        assert_equal "3" [r tbitcount key]
        assert_equal "0" [r tbitfield key incrby u1 2 -1]
        assert_equal "2" [r tbitcount key]
        assert_equal "0" [r tbitfield key set u1 10 1]
        assert_equal "3" [r tbitcount key]
        assert_equal "1" [r tbitcount key 1]
    }

    # random test
    proc RandomRange { min max } {  
        set rd [expr rand()]  
        set result [expr $rd * ($max - $min) + $min]  
        return $result  
    } 
    proc RandomRangeInt { min max } {  
        return [expr int([RandomRange $min $max])]  
    }

    test {tbitfield random} {
        r del key
        for {set i 0} {$i < 1000} {incr i} {
            set offset [RandomRangeInt 0 1000000]
            set value  [RandomRangeInt 0 1000000]
            r tbitfield key set i64 $offset $value
            assert_equal $value [r tbitfield key get i64 $offset]
        }

        r del key
        for {set i 0} {$i < 1000} {incr i} {
            set offset [RandomRangeInt 0 1000000]
            set incr   [RandomRangeInt 0 1000000]
            set value  [r tbitfield key get u32 $offset]
            r tbitfield key overflow sat incrby u32 $offset $incr

            if {[expr $value+$incr] > 4294967295} {
                assert_equal 4294967295 [r tbitfield key get u32 $offset]
            } else {
                assert_equal [expr $value+$incr] [r tbitfield key get u32 $offset]
            }
        }
    }

    # tbitop
    test {tbitop simple} {
        assert_equal "0" [r tsetbit key1 0 1]
        assert_equal "0" [r tsetbit key1 3 1]
        assert_equal "0" [r tsetbit key2 0 1]
        assert_equal "0" [r tsetbit key2 1 1]
        assert_equal "0" [r tsetbit key2 3 1]

        assert_equal "1" [r tbitop and key key1 key2]

        assert_equal "1" [r tgetbit key 0]
        assert_equal "0" [r tgetbit key 1]
        assert_equal "0" [r tgetbit key 2]
        assert_equal "1" [r tgetbit key 3]
    }

    test {tbitop random} {
        r del key key1 key2 key3 ok ok1 ok2 ok3
        # for {set j 1} {$j < 4} {incr j} {
        #     for {set i 0} {$i < 1000} {incr i} {
        #         set offset [RandomRangeInt 0 100000]
        #         set value  [RandomRangeInt 0 100000]
        #         r tbitfield "key$j" set i64 $offset $value
        #         r bitfield "ok$j" set i64 $offset $value
        #     }
        # }

        for {set j 1} {$j < 4} {incr j} {
            for {set i 0} {$i < 2500} {incr i} {
                set offset [RandomRangeInt 0 5000]
                r tsetbit "key$j" $offset 1
                r setbit "ok$j" $offset 1
            }
        }

        assert_equal [r bitop and ok ok1 ok2 ok3] [r tbitop and key key1 key2 key3]
        for {set i 0} {$i < 100000} {incr i} {
            assert_equal [r getbit ok $i] [r tgetbit key $i]
        }

        assert_equal [r bitop or ok ok1 ok2 ok3] [r tbitop or key key1 key2 key3]
        for {set i 0} {$i < 100000} {incr i} {
            assert_equal [r getbit ok $i] [r tgetbit key $i]
        }

        assert_equal [r bitop xor ok ok1 ok2 ok3] [r tbitop xor key key1 key2 key3]
        for {set i 0} {$i < 100000} {incr i} {
            assert_equal [r getbit ok $i] [r tgetbit key $i]
        }

        assert_equal [r bitop not ok ok1] [r tbitop not key key1]
        for {set i 0} {$i < 100000} {incr i} {
            assert_equal [r getbit ok $i] [r tgetbit key $i]
        }
    }
}
