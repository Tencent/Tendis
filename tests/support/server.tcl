set ::global_overrides {}
set ::tags {}
set ::valgrind_errors {}

proc start_server_error {config_file error} {
    set err {}
    append err "Cant' start the Redis server\n"
    append err "CONFIGURATION:\n"
    append err [exec cat $config_file]
    append err "\nERROR:"
    append err [string trim $error]
    send_data_packet $::test_server_fd err $err
}

proc check_valgrind_errors stderr {
    set fd [open $stderr]
    set buf [read $fd]
    close $fd

    if {[regexp -- { at 0x} $buf] ||
        (![regexp -- {definitely lost: 0 bytes} $buf] &&
         ![regexp -- {no leaks are possible} $buf])} {
        send_data_packet $::test_server_fd err "Valgrind error: $buf\n"
    }
}

proc kill_server config {
    # nothing to kill when running against external server
    if {$::external} return
    if {[dict exists $config "slave"]} {
        kill_server [dict get $config slave]
    }
    # nevermind if its already dead
    if {![is_alive $config]} { return }
    set pid [dict get $config pid]

    # check for leaks
    if {![dict exists $config "skipleaks"]} {
        catch {
            if {[string match {*Darwin*} [exec uname -a]]} {
                tags {"leaks"} {
                    test "Check for memory leaks (pid $pid)" {
                        set output {0 leaks}
                        catch {exec leaks $pid} output
                        if {[string match {*process does not exist*} $output] ||
                            [string match {*cannot examine*} $output]} {
                            # In a few tests we kill the server process.
                            set output "0 leaks"
                        }
                        set output
                    } {*0 leaks*}
                }
            }
        }
    }

    # kill server and wait for the process to be totally exited
    puts "kill_server $pid"
    catch {exec kill -9 $pid}
    while {[is_alive $config]} {
        incr wait 10

        if {$wait >= 50000} {
            puts "Forcing process $pid to exit..."
            catch {exec kill -KILL $pid}
        } elseif {$wait % 1000 == 0} {
            puts "Waiting for process $pid to exit..."
        }
        after 10
    }

    # Check valgrind errors if needed
    if {$::valgrind} {
        check_valgrind_errors [dict get $config stderr]
    }
}

proc is_alive config {
    set pid [dict get $config pid]
    if {[catch {exec ps -p $pid} err]} {
        return 0
    } else {
        return 1
    }
}

proc ping_server {host port} {
    set retval 0
    if {[catch {
        set fd [socket $host $port]
        fconfigure $fd -translation binary
        puts $fd "PING\r\n"
        flush $fd
        set reply [gets $fd]
        if {[string range $reply 0 0] eq {+} ||
            [string range $reply 0 0] eq {-}} {
            set retval 1
        }
        close $fd
    } e]} {
        if {$::verbose} {
            puts -nonewline "."
        }
    } else {
        if {$::verbose} {
            puts -nonewline "ok"
        }
    }
    return $retval
}

# Return 1 if the server at the specified addr is reachable by PING, otherwise
# returns 0. Performs a try every 50 milliseconds for the specified number
# of retries.
proc server_is_up {host port retrynum} {
    after 10 ;# Use a small delay to make likely a first-try success.
    set retval 0
    while {[incr retrynum -1]} {
        if {[catch {ping_server $host $port} ping]} {
            set ping 0
        }
        if {$ping} {return 1}
        after 50
    }
    return 0
}

proc wait_for_binlog_ready {mcli scli} {
    after 1000
    for {set i 0} {$i<10} {incr i 1} {
        set ret [catch {$mcli binlogpos $i} master_pos]
        if {$ret != 0} {
            error $ret $::errorInfo
        }
        set slave_pos 0
        while {$slave_pos ne $master_pos} {
            if {[catch {$scli binlogpos $i} slave_pos] != 0} {
                error $::errorInfo
            }
            after 1000
            puts "store $i get master_pos $master_pos slave_pos $slave_pos not equal"
        }
        puts "store $i get master_pos $master_pos slave_pos $slave_pos"
    }
}

# doesn't really belong here, but highly coupled to code in start_server
proc tags {tags code} {
    set ::tags [concat $::tags $tags]
    uplevel 1 $code
    set ::tags [lrange $::tags 0 end-[llength $tags]]
}

proc start_server {options {code undefined}} {
    # If we are running against an external server, we just push the
    # host/port pair in the stack the first time
    if {$::external} {
        if {[llength $::servers] == 0} {
            set srv {}
            dict set srv "host" $::host
            dict set srv "port" $::port
            set client [redis $::host $::port]
            dict set srv "client" $client
            $client select 9

            # append the server to the stack
            lappend ::servers $srv
        }
        uplevel 1 $code
        return
    }

    # setup defaults
    set baseconfig "tendisplus.conf"
    set overrides {}
    set tags {}

    # parse options
    foreach {option value} $options {
        switch $option {
            "config" {
                set baseconfig $value }
            "overrides" {
                set overrides $value }
            "tags" {
                set tags $value
                set ::tags [concat $::tags $value] }
            default {
                error "Unknown option $option" }
        }
    }

    set data [split [exec cat "tests/assets/$baseconfig"] "\n"]
    set config {}
    foreach line $data {
        if {[string length $line] > 0 && [string index $line 0] ne "#"} {
            set elements [split $line " "]
            set directive [lrange $elements 0 0]
            set arguments [lrange $elements 1 end]
            dict set config $directive $arguments
        }
    }

    # use a different directory every time a server is started
    dict set config dir [tmpdir server]
    set slave_dir [tmpdir slave]

    # start every server on a different port
    set ::port [find_available_port [expr {$::port+1}]]

    set slave_port [find_available_port [expr {$::port+1}]]

    # apply overrides from global space and arguments
    foreach {directive arguments} [concat $::global_overrides $overrides] {
        dict set config $directive $arguments
    }

    # write new configuration to temporary file
    set config_file [tmpfile tendisplus.conf]
    set slave_cfg_file [tmpfile tendisplus.conf]
    set fp [open $config_file w+]
    set fp2 [open $slave_cfg_file w+]
    set slave_cfg {}
    set break 0
    if {[string compare "limits" [lindex $tags 0]]} {
        set break 1
    } 
    if {[string compare "auth" [lindex $tags 0]]} {
        set break 1
    }
    foreach directive [dict keys $config] {
        puts -nonewline $fp "$directive "
        puts $fp [dict get $config $directive]

        puts -nonewline $fp2 "$directive "
        if {$directive eq {logdir} || $directive eq {dumpdir} || $directive eq {pidfile}} {
            dict set slave_cfg $directive [dict get $config $directive]_slave
            puts $fp2 "[dict get $config $directive]_slave"
        } elseif {$directive eq {dir}} {
            dict set slave_cfg dir $slave_dir
            puts $fp2 $slave_dir
        } else {
            dict set slave_cfg $directive [dict get $config $directive]
            puts $fp2 [dict get $config $directive]
        }
    }

    dict set config port $::port
    dict set slave_cfg port $slave_port

    # write host and port into config file
    puts -nonewline $fp "bind "
    puts $fp $::host
    puts -nonewline $fp "port "
    puts $fp $::port
    close $fp

    puts -nonewline $fp2 "bind "
    puts $fp2 $::host
    puts -nonewline $fp2 "port "
    puts $fp2 $slave_port
    close $fp2

    set stdout [format "%s/%s" [dict get $config "dir"] "stdout"]
    set systemTime [clock seconds]
    set tendis_master_mem_log [tmpfile tendis_master_mem_$systemTime.log]
    set tendis_slave_mem_log [tmpfile tendis_slave_mem_$systemTime.log]
    
    if {$::valgrind} {
        exec valgrind --suppressions=src/valgrind.sup --show-reachable=no --show-possibly-lost=no --leak-check=full --soname-synonyms=somalloc=jemalloc ./build/bin/tendisplus $config_file > $stdout 2> $tendis_slave_mem_log &
        #exec valgrind --log-file=./valgrind.log --show-reachable=no --show-possibly-lost=no --leak-check=full --show-leak-kinds=all --soname-synonyms=somalloc=jemalloc ./build/bin/tendisplus $config_file > $stdout 2> $tendis_slave_mem_log &
    } else {
        exec ./build/bin/tendisplus $config_file > $stdout 2> $tendis_master_mem_log &
    }
    exec ./build/bin/tendisplus $slave_cfg_file > $stdout 2> $tendis_slave_mem_log &

    # check that the server actually started
    # ugly but tries to be as fast as possible...
    if {$::valgrind} {set retrynum 1000} else {set retrynum 100}

    if {$::verbose} {
        puts -nonewline "=== ($tags) Starting server ${::host}:${::port} "
    }

    if {$code ne "undefined"} {
        set serverisup [server_is_up $::host $::port $retrynum]
    } else {
        set serverisup 1
    }

    if {$::verbose} {
        puts ""
    }

    if {!$serverisup} {
        set err {}
        append err [exec cat $stdout] "\n" [exec cat $tendis_slave_mem_log]
        start_server_error $config_file $err
        return
    }

    # find out the pid
    after 100
    set pidfile [dict get $config "pidfile"]
    wait_for_condition 100 500 {
        [file exists $pidfile] == 1
    } else {
        fail "start_server cant find $pidfile."
    }
    set pid [exec cat $pidfile]

    set pidfile2 [dict get $slave_cfg "pidfile"]
    wait_for_condition 100 500 {
        [file exists $pidfile2] == 1
    } else {
        fail "start_server cant find $pidfile2."
    }
    set slave_pid [exec cat $pidfile2]

    # setup properties to be able to initialize a client object
    set host $::host
    set port $::port
    if {[dict exists $config bind]} { set host [dict get $config bind] }
    if {[dict exists $config port]} { set port [dict get $config port] }

    # setup config dict
    dict set srv "config_file" $config_file
    dict set srv "config" $config
    dict set srv "pid" $pid
    dict set srv "host" $host
    dict set srv "port" $port
    dict set srv "stdout" $stdout

    dict set slave "config_file" $slave_cfg_file
    dict set slave "config" $slave_cfg
    dict set slave "pid" $slave_pid
    dict set slave "host" $host
    dict set slave "port" $slave_port
    dict set slave "stdout" $stdout
    dict set srv "slave" $slave

    # if a block of code is supplied, we wait for the server to become
    # available, create a client object and kill the server afterwards
    if {$code ne "undefined"} {
        set line [exec head -n1 $stdout]
        if {[string match {*already in use*} $line]} {
            error_and_quit $config_file $line
        }

        set logdir [dict get $config logdir]
        set infofile "/tendisplus.WARNING"
        puts $logdir$infofile
        while 1 {
            # check that the server actually started and is ready for connections
            if {[exec grep "ready to accept" | wc -l < $logdir$infofile] > 0} {
                break
            }
            after 10
        }

        if {$break == 0} {
             # create a client of slave
            set scli [redis $host $slave_port]
            dict set slave "client" $scli
            set args "slaveof $host $port"
            puts "$slave_port $args"
            $scli {*}$args
        }
       
        # append the server to the stack
        lappend ::servers $srv

        # connect client (after server dict is put on the stack)
        reconnect
        set mcli [redis $host $port]

        # execute provided block
        set num_tests $::num_tests
        if {[catch { uplevel 1 $code } error]} {
            set backtrace $::errorInfo

            # Kill the server without checking for leaks
            dict set srv "skipleaks" 1
            kill_server $srv

            # Print warnings from log
            puts [format "\nLogged warnings (pid %d):" [dict get $srv "pid"]]
            set warnings [warnings_from_file [dict get $srv "stdout"]]
            if {[string length $warnings] > 0} {
                puts "$warnings"
            } else {
                puts "(none)"
            }
            puts ""

            error $error $backtrace
        }

        # Don't do the leak check when no tests were run
        if {$num_tests == $::num_tests} {
            dict set srv "skipleaks" 1
        }

        # pop the server object
        set ::servers [lrange $::servers 0 end-1]

        set ::tags [lrange $::tags 0 end-[llength $tags]]

        if {$break == 0} {
            # execute keys command to expire expired keys
            if {[catch {for {set i 0} {$i < 16} {incr i} {
                $mcli select $i
                $mcli keys *
            }} err]} {
                kill_server $srv
                error $err
            }

            # wait for slave applying binlog
            if {[catch {wait_for_binlog_ready $mcli $scli} err]} {
                kill_server $srv
                error $err
            }

            # compare -addr1 {tendis ip:port} -addr2 {redis ip:port} cause only tendis support iterall.
            set slv [dict get $srv slave]
            set src "[dict get $srv host]:[dict get $srv port]"
            set dst "[dict get $slv host]:[dict get $slv port]"
            if {[file exists ./tests/compare] == 0} {
                kill_server $srv
                error "can't find binary file: ./tests/compare"
            }
            set retcode [catch {exec ./tests/compare $src $dst} result]
            if {$retcode != 0} {
                kill_server $srv
                error $retcode $result
            } else {
                puts "\[[colorstr green ok]\]: COMPARE $result"
            }
        }
        
        kill_server $srv
    } else {
        set ::tags [lrange $::tags 0 end-[llength $tags]]
        set _ $srv
    }
}
