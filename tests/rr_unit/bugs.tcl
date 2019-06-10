start_server {tags {"basic"}} {
    #test { TRedis random key # no dataset} {
    #    r flushalldisk
    #    r randomkey
    # } {}
     
    # test { TRedis random key # db not existed} {
    #    r flushalldisk
    #    r set a b
    #    r select 10
    #    r randomkey
    #} {}

    #test {TRedis random key # entries less than rand} {
    #    r flushalldisk
    #    r select 9
    #    r set a b
    #    r set b c
    #
    #    for {set j 0} {$j < 2} {incr j} {
    #      r lpush L hello
    #    }
    #
    #    r select 10
    #    set c b
    #    set d c

    #    r select 9
    #    for {set j 0} {$j < 1024} {incr j} {
    #      set rand [r randomkey]
    #      assert {[string equal $rand "a"] || [string equal $rand "b"] || [string equal $rand "L"]}
    #    }
    #}

    #test { TRedis random key # entries more than rand} {
    #    r flushalldisk
    #    r select 9
    #    r set a b
    #    r set b c
        
    #    for {set j 0} {$j < 1024} {incr j} {
    #      r lpush L hello
    #    }
        
    #    r select 10
    #    set c b
    #    set d c

    #    r select 9
    #    for {set j 0} {$j < 1024} {incr j} {
    #      set rand [r randomkey]
    #      assert {[string equal $rand a] || [string equal $rand b] || [string equal $rand L]}
    #    }
    #}
}
