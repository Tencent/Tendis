start_server {tags {"bg stats big"}} {
    set ops 50000 
    test {STATS BACKGROUND - stats big} {
        for {set j 0} {$j < $ops} {incr j} {
            r set $j $j
            r lpush $j $j 
            r hset $j $j $j
            r sadd $j 1 $j
        }
        
        for {set j 0} {$j < $ops} {incr j} {
            r expire $j 20
        }

        wait_stop 300000 10000
        assert_match "*string_count: 50000 list_count: 50000 hash_count: 50000 set_count: 50000 zset_count: 0 expired_count: 0 expiring_count: 200000*" [r typestats]
        set size [r rrdbsize]
        while {$size ne 0} {
            after 10000 
            set size [r rrdbsize]
            if {$size eq 0} { break }
        } 
        assert_match "*string_count: 0 list_count: 0 hash_count: 0 set_count: 0 zset_count: 0 expired_count: 0 expiring_count: 0*" [r typestats]
    } {}
}

start_server {tags {"bg stats small"}} {
     set ops 10000 
     test {STATS BACKGROUND - stats small} {
       for {set j 0} {$j < $ops} {incr j} {
            r set $j $j
            r lpush $j $j 
            r hset $j $j $j
            r sadd $j 1 $j
         }

         for {set j 0} {$j < $ops} {incr j} {
            r expire $j 10
         }

         after 10000
         assert_match "*string_count: 10000 list_count: 10000 hash_count: 10000 set_count: 10000 zset_count: 0 expired_count: 0 expiring_count: 40000*" [r typestats]
         set size [r rrdbsize]
         while {$size ne 0} {
           after 10000 
           set size [r rrdbsize]
           if {$size eq 0} { break }
         } 
         assert_match "*string_count: 0 list_count: 0 hash_count: 0 set_count: 0 zset_count: 0 expired_count: 0 expiring_count: 0*" [r typestats]
     } {}
}

start_server {tags {"bg stats tiny"}} {
    set ops 5000

    test {STATS BACKGROUND - stats tiny} {
        for {set j 0} {$j < $ops} {incr j} {
            r set $j $j
            r lpush $j $j 
            r hset $j $j $j
            r zadd $j 1 $j
        }
        
        for {set j 0} {$j < $ops} {incr j} {
            r expire $j 10
        }

        wait_stop 60000 10000
        assert_match "*string_count: 5000 list_count: 5000 hash_count: 5000 set_count: 0 zset_count: 5000 expired_count: 0 expiring_count: 20000*" [r typestats]
        set size [r rrdbsize]
        while {$size ne 0} {
            after 10000 
            set size [r rrdbsize]
            if {$size eq 0} { break }
        } 
        assert_match "*string_count: 0 list_count: 0 hash_count: 0 set_count: 0 zset_count: 0 expired_count: 0 expiring_count: 0*" [r typestats]
    } {}
}
