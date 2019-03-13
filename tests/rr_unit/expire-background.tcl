start_server {tags {"expire"}} {
    set ops 5000

    test {EXPIRE BACKGROUND - all expired keys deleted} {
        for {set j 0} {$j < $ops} {incr j} {
            r set $j $j
            r lpush $j $j 
            r hset $j $j $j
        }
        
        for {set j 0} {$j < $ops} {incr j} {
            r expire $j 10
        }

        set size [r rrdbsize]
        while {$size ne 0} {
            after 10000 
            set size [r rrdbsize]
            if {$size eq 0} { break }
        } 

        r rrdbsize
    } {0}
    

    test {EXPIRE BACKGROUND - expired but not deleted keys overwritted} {
        r setex zfoo 60 zbar

        for {set j 0} {$j < $ops} {incr j} {
            r set $j $j
            r lpush $j $j 
            r hset $j $j $j
        }
         
        for {set j 0} {$j < $ops} {incr j} {
            r expire $j 60 
        }

        after 40000
        r set zfoo zbar
        r get zfoo 
    } {zbar}
}
