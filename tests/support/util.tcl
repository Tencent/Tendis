proc randstring {min max {type binary}} {
    set len [expr {$min+int(rand()*($max-$min+1))}]
    set output {}
    if {$type eq {binary}} {
        set minval 0
        set maxval 255
    } elseif {$type eq {alpha}} {
        set minval 48
        set maxval 122
    } elseif {$type eq {compr}} {
        set minval 48
        set maxval 52
    }
    while {$len} {
        append output [format "%c" [expr {$minval+int(rand()*($maxval-$minval+1))}]]
        incr len -1
    }
    return $output
}

# Useful for some test
proc zlistAlikeSort {a b} {
    if {[lindex $a 0] > [lindex $b 0]} {return 1}
    if {[lindex $a 0] < [lindex $b 0]} {return -1}
    string compare [lindex $a 1] [lindex $b 1]
}

# Return all log lines starting with the first line that contains a warning.
# Generally, this will be an assertion error with a stack trace.
proc warnings_from_file {filename} {
    set lines [split [exec cat $filename] "\n"]
    set matched 0
    set logall 0
    set result {}
    foreach line $lines {
        if {[string match {*REDIS BUG REPORT START*} $line]} {
            set logall 1
        }
        if {[regexp {^\[\d+\]\s+\d+\s+\w+\s+\d{2}:\d{2}:\d{2} \#} $line]} {
            set matched 1
        }
        if {$logall || $matched} {
            lappend result $line
        }
    }
    join $result "\n"
}

# Return value for INFO property
proc status {r property} {
    if {[regexp "\r\n$property:(.*?)\r\n" [{*}$r info] _ value]} {
        set _ $value
    }
}

proc waitForBgsave r {
    while 1 {
        if {[status r rdb_bgsave_in_progress] eq 1} {
            if {$::verbose} {
                #puts -nonewline "\nWaiting for background save to finish... "
                flush stdout
            }
            after 1000
        } else {
            break
        }
    }
}

proc waitForBgrewriteaof r {
    while 1 {
        if {[status r aof_rewrite_in_progress] eq 1} {
            if {$::verbose} {
                #puts -nonewline "\nWaiting for background AOF rewrite to finish... "
                flush stdout
            }
            after 1000
        } else {
            break
        }
    }
}

proc wait_for_sync r {
    while 1 {
        if {[status $r master_link_status] eq "down"} {
            after 10
        } else {
            break
        }
    }
}

# Random integer between 0 and max (excluded).
proc randomInt {max} {
    expr {int(rand()*$max)}
}

# Random signed integer between -max and max (both extremes excluded).
proc randomSignedInt {max} {
    set i [randomInt $max]
    if {rand() > 0.5} {
        set i -$i
    }
    return $i
}

proc randpath args {
    set path [expr {int(rand()*[llength $args])}]
    uplevel 1 [lindex $args $path]
}

proc randomValue {} {
    randpath {
        # Small enough to likely collide
        randomSignedInt 1000
    } {
        # 32 bit compressible signed/unsigned
        randpath {randomSignedInt 2000000000} {randomSignedInt 4000000000}
    } {
        # 64 bit
        randpath {randomSignedInt 1000000000000}
    } {
        # Random string
        randpath {randstring 0 256 alpha} \
                {randstring 0 256 compr} \
                {randstring 0 256 binary}
    }
}

proc randomKey {} {
    randpath {
        # Small enough to likely collide
        randomInt 1000
    } {
        # 32 bit compressible signed/unsigned
        randpath {randomInt 2000000000} {randomInt 4000000000}
    } {
        # 64 bit
        randpath {randomInt 1000000000000}
    } {
        # Random string
        randpath {randstring 1 256 alpha} \
                {randstring 1 256 compr} \
                {randstring 0 256 binary}
    }
}

proc findKeyWithType {r type} {
    for {set j 0} {$j < 20} {incr j} {
        set k [{*}$r randomkey]
        if {$k eq {}} {
            return {}
        }
        if {[{*}$r type $k] eq $type} {
            return $k
        }
    }
    return {}
}

proc createComplexDataset {r ops {opt []}} {
    set tredis 0
    if {[lsearch $opt tredis] != -1} {
        set tredis 1
    }

    for {set j 0} {$j < $ops} {incr j} {
        set k [randomKey]
        set k2 [randomKey]
        set f [randomValue]
        set v [randomValue]

        if {[lsearch $opt useexpire] != -1} {
            if {rand() < 0.1} {
                set t [randomInt 2]
                #puts [concat "expire" $k $t]
                {*}$r expire $k $t
            }
        }

        randpath {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            randpath {set d +inf} {set d -inf}
        }
        set t [{*}$r type $k]

        if {$t eq {none}} {
            randpath {
                #puts [concat "set" $k $v]
                {*}$r set $k $v
            } {
                #puts [concat "lpush" $k $v]
                {*}$r lpush $k $v
            } {
                #puts [concat "sadd" $k $v]
                #{*}$r sadd $k $v
            } {
                #puts [concat "zadd" $k $d $v]
                {*}$r zadd $k $d $v
            } {
                #puts [concat "hset" $k $f $v]
                {*}$r hset $k $f $v
            } {
                #puts [concat "del" $k ]
                {*}$r del $k
            } {
                if {$tredis eq 1} {
                  {*}$r hmcasv2 $k 0 -1 999 $f 0 $v $v 0 $f $k2 0 $v $k 0 $v
              }
            }
            set t [{*}$r type $k]
        }

        switch $t {
            {string} {
                # Nothing to do
            }
            {list} {
                randpath {
                    #puts [concat "lpush" $k $v]
                    {*}$r lpush $k $v} \
                    {
                    #puts [concat "rpush" $k $v]
                    {*}$r rpush $k $v} \
                        {
                    #puts [concat "rpop" $k ]
                            {*}$r rpop $k} \
                        {
                    #puts [concat "lpop" $k ]
                            {*}$r lpop $k}
                        # {{*}$r lrem $k 0 $v}
            }
            {set} {
                #randpath {{*}$r sadd $k $v} \
                #        {{*}$r srem $k $v}
                        #{
                        #    set otherset [findKeyWithType {*}$r set]
                        #    if {$otherset ne {}} {
                        #        randpath {
                        #            #puts [concat "sunionstore" $k2 $k $otherset]
                        #            #{*}$r sunionstore $k2 $k $otherset
                        #        } {
                        #            #puts [concat "sinterstore" $k2 $k $otherset]
                        #            #{*}$r sinterstore $k2 $k $otherset
                        #        } {
                        #            #puts [concat "sdiffstore" $k2 $k $otherset]
                        #            #{*}$r sdiffstore $k2 $k $otherset
                        #        }
                        #    }
                        #}
            }
            {zset} {
                randpath {{*}$r zadd $k $d $v} \
                        {{*}$r zrem $k $v}
                        #{
                        #    set otherzset [findKeyWithType {*}$r zset]
                        #    if {$otherzset ne {}} {
                        #        randpath {
                        #            #puts [concat "zunionstore" $k2 $k $otherzset]
                        #            {*}$r zunionstore $k2 2 $k $otherzset
                        #        } {
                        #            #puts [concat "zinterstore" $k2 $k $otherzset]
                        #            {*}$r zinterstore $k2 2 $k $otherzset
                        #        }
                        #    }
                        #}
            }
            {hash} {
                randpath {{*}$r hset $k $f $v} \
                        {{*}$r hdel $k $f}
            }
        }
    }
}

proc debugPopulateKeys {r count {type "string"} {prefix "key"}} {
  set type [string tolower $type]
  for {set idx 0} {$idx < $count} {incr idx} {
    set key "key:${idx}"
    if {[string compare prefix ""] != 0} {
      set key "${prefix}:${idx}"                          
    }
    set value "value:${idx}"
    switch $type {
        "string" {
            set string_key "${key}"
            {*}$r set $string_key $value                                
        }
        "list" {
            set list_key "${key}_list"
            {*}$r lpush $list_key $value
        }
        "hash" {
            set hash_key "${key}_hash"
            set field "field:${idx}"
            {*}$r hset $hash_key $field $value
        }
        "set" {
            set set_key "${key}_set"
            {*}$r sadd $set_key $value
        }
        "zset" {
            set zset_key "${key}_zset"
            set score "0.0"
            {*}$r zadd $zset_key $score $value
        }
        default {
            puts "Unknown populate keys type"
        }
    }
  }
}

proc formatCommand {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args {
        append cmd "$[string length $a]\r\n$a\r\n"
    }
    set _ $cmd
}

proc comparedb {r1 r2} {
    set o {}
    foreach k [lsort [{*}$r1 keys *]] {
        set type [{*}$r1 type $k]
        switch $type {
            string {
                set v1 [{*}$r1 get $k]
                set v2 [{*}$r2 get $k]
                if {$v1 ne $v2} {
                    append o [csvstring "string"]
                    append o [csvstring $k]
                    append o "\r\n"
                }

                break
            }
            list {
                set identical 1
                set l1 [lsort [{*}$r1 lrange $k 0 -1]]
                set l2 [lsort [{*}$r2 lrange $k 0 -1]]
                if {[llength $l1] eq [llength $l2]} {
                    set len [llength $l1]
                    for {set index 0} {$index < $len} {incr index} {
                        if { [lindex $l1 $index] ne [lindex $l2 $index]} {
                            set identical 0
                            break
                        }
                    }
                } else {
                    # track inconsistent list element
                    set identical 0
                    break
                }

                if { $identical ne 1 } {
                    append o [csvstring "list"]
                    append o [csvstring $k]
                    append o "\r\n"
                }

                break
            }
            set {
                set identical 1
                set s1 [lsort [[{*}$r1 smembers $k]]
                set s2 [lsort [{*}$r2 smembers $k]]
                if {[llength $s1] eq [llength $s2]} {
                    set len [llength $s1]
                    for {set index 0} {$index < $len} {incr index} {
                        if { [lindex $s1 $index] ne [lindex $s2 $index]} {
                            set identical 0
                            break
                        }
                  }
                } else {
                    set identical 0
                }

                if { $identical ne 1 } {
                    append o [csvstring "set"]
                    append o [csvstring $k]
                    append o "\r\n"
                }

                break
            }
            zset {
                set identical 1
                set z1 [lsort [{*}$r1 zrange $k 0 -1]]
                set z2 [lsort [{*}$r2 zrange $k 0 -1]]
                if {[llength $z1] eq [llength $z2]} {
                    set len [llength $z1]
                    for {set index 0} {$index < $len} {incr index} {
                        if { [lindex $z1 $index] ne [lindex $z2 $index]} {
                            set identical 0
                            break
                        }
                  }
                } else {
                    set identical 0
                }

                if { $identical ne 1 } {
                    append o [csvstring "zset"]
                    append o [csvstring $k]
                    append o "\r\n"
                }

                break
            }
            hash {
                set identical 1
                set h1 [lsort [{*}$r1 hkeys $k]]
                set h2 [lsort [{*}$r2 hkeys $k]]
                if {[llength $h1] eq [llength $h2]} {
                    set len [llength $h1]
                    for {set index 0} {$index < $len} {incr index} {
                        if { [lindex $h1 $index] ne [lindex $h2 $index]} {
                            set identical 0
                            break
                        }
                  }
                } else {
                    set identical 0
                }

                if {$identical ne 1} {
                    append o [csvstring "hash"]
                    append o [csvstring $k]
                    append o "\r\n"
                }

                break
            }
        }

        if {[llength $o] ne 0} {
            set fd [open /tmp/repldump.txt w]
            puts -nonewline $fd $o
            close $fd
        }
    }
}

proc csvdump r {
    set o {}
    foreach k [lsort [{*}$r keys *]] {
        set type [{*}$r type $k]
        set ttl [{*}$r ttl $k]
        append o [csvstring $k] , [csvstring $type] ,
        switch $type {
            string {
                append o [csvstring [{*}$r get $k]]
            }
            list {
                foreach e [{*}$r lrange $k 0 -1] {
                    append o [csvstring $e] ,
                }
            }
            set {
                foreach e [lsort [{*}$r smembers $k]] {
                    append o [csvstring $e] ,
                }
            }
            zset {
                foreach e [{*}$r zrange $k 0 -1 withscores] {
                    append o [csvstring $e] ,
                }
            }
            hash {
                set fields [{*}$r hgetall $k]
                set newfields {}
                foreach {k v} $fields {
                    lappend newfields [list $k $v]
                }
                set fields [lsort -index 0 $newfields]
                foreach kv $fields {
                    append o [csvstring [lindex $kv 0]] ,
                    append o [csvstring [lindex $kv 1]] ,
                }
            }
        }
        append o [csvstring $ttl]
        append o "\n"
    }
    return $o
}

proc csvstring s {
    return "\"$s\""
}

proc roundFloat f {
    format "%.10g" $f
}

proc find_available_port start {
    for {set j $start} {$j < $start+1024} {incr j} {
        if {[catch {
            set fd [socket 127.0.0.1 $j]
        }]} {
            return $j
        } else {
            close $fd
        }
    }
    if {$j == $start+1024} {
        error "Can't find a non busy port in the $start-[expr {$start+1023}] range."
    }
}

# Test if TERM looks like to support colors
proc color_term {} {
    expr {[info exists ::env(TERM)] && [string match *xterm* $::env(TERM)]}
}

proc colorstr {color str} {
    if {[color_term]} {
        set b 0
        if {[string range $color 0 4] eq {bold-}} {
            set b 1
            set color [string range $color 5 end]
        }
        switch $color {
            red {set colorcode {31}}
            green {set colorcode {32}}
            yellow {set colorcode {33}}
            blue {set colorcode {34}}
            magenta {set colorcode {35}}
            cyan {set colorcode {36}}
            white {set colorcode {37}}
            default {set colorcode {37}}
        }
        if {$colorcode ne {}} {
            return "\033\[$b;${colorcode};49m$str\033\[0m"
        }
    } else {
        return $str
    }
}

# Execute a background process writing random data for the specified number
# of seconds to the specified Redis instance.
proc start_write_load {host port seconds} {
    set tclsh [info nameofexecutable]
    exec $tclsh tests/helpers/gen_write_load.tcl $host $port $seconds &
}

# Stop a process generating write load executed with start_write_load.
proc stop_write_load {handle} {
    catch {exec /bin/kill -9 $handle}
}

proc wait_sync {accurate_sync_wait_time_valgrind
  accurate_sync_wait_time
  sync_wait_time_valgrind
  sync_wait_time} {
  if { $::valgrind } {
    if { $::accurate } {
      after $accurate_sync_wait_time_valgrind
    } else {
      after $sync_wait_time_valgrind
    }
  } else {
    if { $::accurate } {
      after $accurate_sync_wait_time
    } else {
      after $sync_wait_time
    }
  }
}

proc wait_stop {stop_wait_time_valgrind stop_wait_time} {
  if { $::valgrind } {
    after $stop_wait_time_valgrind
  } else {
    after $stop_wait_time
  }
}

proc start_bg_complex_data {host port db ops} {
  set tclsh [info nameofexecutable]
  exec $tclsh tests/helpers/bg_complex_data.tcl $host $port $db $ops &
}

proc stop_bg_complex_data {handle} {
  catch {exec /bin/kill -9 $handle}
}
