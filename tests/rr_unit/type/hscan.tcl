start_server {tags {"scan"}} {
     test "HSCAN" {
         # Create the Hash
         r del hash
         set count 1000
         set elements {}

         for {set j 0} {$j < $count} {incr j} {
             lappend elements key:$j $j
         }
         r hmset hash {*}$elements

         # Test HSCAN
         set cur ""
         set keys {}
         while 1 {
             set res [r hscan hash $cur]
             set cur [lindex $res 0]
             set k [lindex $res 1]
             lappend keys {*}$k
             if {$cur == ""} break
         }

         set keys2 {}
         foreach {k v} $keys {
             assert {$k eq "key:$v"}
             lappend keys2 $k
         }

         set keys2 [lsort -unique $keys2]
         assert_equal $count [llength $keys2]
     }

     test "HSCAN with iteration count limit and time limit" {
         r del ahash
         set count 100000
         set elements {}

         for {set j 0} {$j < 128} {incr j} {
             lappend elements akey:$j $j
         }
         r hmset ahash {*}$elements
         for {set j 0} {$j < $count} {incr j} {
              r hset ahash key:$j $j
         }

         r config set scan-iter-time 1000
         set cur ""
         set keys {}
         set res [r hscan ahash $cur]
         set k [lindex $res 1]
         lappend keys {*}$k

         set keys2 {}
         foreach {k v} $keys {
             assert {$k eq "akey:$v"}
             lappend keys2 $k
         }

         set keys2 [lsort -unique $keys2]
         assert_equal 128 [llength $keys2]

         r del ahash
         r config set scan-iter-time 10
     }

     test "HSCAN with wrong parameter type" {
        r del mykey
        r hmset mykey foo 1 fab 2 fiz 3 foobar 10 1 a 2 b 3 c 4 d
        assert_error "*syntax error*" {r hscan mykey "" MATCH foo* COUNT}
     }

     test "HSCAN with string cursor among keys" {
        r del mykey
        r hmset mykey foo 1 fab 2 fiz 3 foobar 10 cooo 1 coooo 2
        set res [r hscan mykey d COUNT 100]
        lsort -unique [lindex $res 1]
     } {1 10 2 3 fab fiz foo foobar}

     test "HSCAN with string cursor after all keys" {
        r del mykey
        r hmset mykey foo 1 fab 2 fiz 3 foobar 10 cooo 1 coooo 2
        set res [r hscan mykey g COUNT 100]
        lsort -unique [lindex $res 1]
      } {}

     test "HSCAN with string cursor before all keys" {
        r del mykey
        r hmset mykey foo 1 fab 2 fiz 3 foobar 10 cooo 1 coooo 2
        set res [r hscan mykey a COUNT 100]
        lsort -unique [lindex $res 1]
     } {1 10 2 3 cooo coooo fab fiz foo foobar}

     test "HSCAN with PATTERN" {
         r del mykey
         r hmset mykey foo 1 fab 2 fiz 3 foobar 10 1 a 2 b 3 c 4 d
         set res [r hscan mykey "" MATCH foo* COUNT 10000]
         lsort -unique [lindex $res 1]
     } {1 10 foo foobar}
}
