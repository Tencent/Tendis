start_server {tags {"hash"}} {
    test {HSET/HLEN - Small hash creation} {
        array set smallhash {}
        for {set i 0} {$i < 8} {incr i} {
            set key [randstring 0 8 alpha]
            set val [randstring 0 8 alpha]
            if {[info exists smallhash($key)]} {
                incr i -1
                continue
            }
            r hset smallhash $key $val
            set smallhash($key) $val
        }
        list [r hlen smallhash]
    } {8}

    test {Is the small hash encoded with a ziplist?} {
        #assert_encoding ziplist smallhash
    }

    test {HSET/HLEN - Big hash creation} {
        array set bighash {}
        for {set i 0} {$i < 1024} {incr i} {
            set key [randstring 0 8 alpha]
            set val [randstring 0 8 alpha]
            if {[info exists bighash($key)]} {
                incr i -1
                continue
            }
            r hset bighash $key $val
            set bighash($key) $val
        }
        list [r hlen bighash]
    } {1024}

    test {Is the big hash encoded with a ziplist?} {
        #assert_encoding hashtable bighash
    }

    test {HGET against the small hash} {
        set err {}
        foreach k [array names smallhash *] {
            if {$smallhash($k) ne [r hget smallhash $k]} {
                set err "$smallhash($k) != [r hget smallhash $k]"
                break
            }
        }
        set _ $err
    } {}

    test {HGET against the big hash} {
        set err {}
        foreach k [array names bighash *] {
            if {$bighash($k) ne [r hget bighash $k]} {
                set err "$bighash($k) != [r hget bighash $k]"
                break
            }
        }
        set _ $err
    } {}

    test {HGET against non existing key} {
        set rv {}
        lappend rv [r hget smallhash __123123123__]
        lappend rv [r hget bighash __123123123__]
        set _ $rv
    } {{} {}}

    test {HSET in update and insert mode} {
        set rv {}
        set k [lindex [array names smallhash *] 0]
        lappend rv [r hset smallhash $k newval1]
        set smallhash($k) newval1
        lappend rv [r hget smallhash $k]
        lappend rv [r hset smallhash __foobar123__ newval]
        set k [lindex [array names bighash *] 0]
        lappend rv [r hset bighash $k newval2]
        set bighash($k) newval2
        lappend rv [r hget bighash $k]
        lappend rv [r hset bighash __foobar123__ newval]
        lappend rv [r hdel smallhash __foobar123__]
        lappend rv [r hdel bighash __foobar123__]
        set _ $rv
    } {0 newval1 1 0 newval2 1 1 1}

    test {HSETNX target key missing - small hash} {
        r hsetnx smallhash __123123123__ foo
        r hget smallhash __123123123__
    } {foo}

    test {HSETNX target key exists - small hash} {
        r hsetnx smallhash __123123123__ bar
        set result [r hget smallhash __123123123__]
        r hdel smallhash __123123123__
        set _ $result
    } {foo}

    test {HSETNX target key missing - big hash} {
        r hsetnx bighash __123123123__ foo
        r hget bighash __123123123__
    } {foo}

    test {HSETNX target key exists - big hash} {
        r hsetnx bighash __123123123__ bar
        set result [r hget bighash __123123123__]
        r hdel bighash __123123123__
        set _ $result
    } {foo}

    test {HMSET wrong number of args} {
        catch {r hmset smallhash key1 val1 key2} err
        format $err
    } {*wrong number*}

    test {HMSET - small hash} {
        set args {}
        foreach {k v} [array get smallhash] {
            set newval [randstring 0 8 alpha]
            set smallhash($k) $newval
            lappend args $k $newval
        }
        r hmset smallhash {*}$args
    } {OK}

    test {HMSET - big hash} {
        set args {}
        foreach {k v} [array get bighash] {
            set newval [randstring 0 8 alpha]
            set bighash($k) $newval
            lappend args $k $newval
        }
        r hmset bighash {*}$args
    } {OK}

    test {HMGET against non existing key and fields} {
        set rv {}
        lappend rv [r hmget doesntexist __123123123__ __456456456__]
        lappend rv [r hmget smallhash __123123123__ __456456456__]
        lappend rv [r hmget bighash __123123123__ __456456456__]
        set _ $rv
    } {{{} {}} {{} {}} {{} {}}}

    test {HMGET against wrong type} {
        r set wrongtype somevalue
        #assert_error "*wrong*" {r hmget wrongtype field1 field2}
        r hmget wrongtype field1 field2
    } {{} {}}

    test {HMGET - small hash} {
        set keys {}
        set vals {}
        foreach {k v} [array get smallhash] {
            lappend keys $k
            lappend vals $v
        }
        set err {}
        set result [r hmget smallhash {*}$keys]
        if {$vals ne $result} {
            set err "$vals != $result"
            break
        }
        set _ $err
    } {}

    test {HMGET - big hash} {
        set keys {}
        set vals {}
        foreach {k v} [array get bighash] {
            lappend keys $k
            lappend vals $v
        }
        set err {}
        set result [r hmget bighash {*}$keys]
        if {$vals ne $result} {
            set err "$vals != $result"
            break
        }
        set _ $err
    } {}

    test {HKEYS - small hash} {
        lsort [r hkeys smallhash]
    } [lsort [array names smallhash *]]

    test {HKEYS - big hash} {
        lsort [r hkeys bighash]
    } [lsort [array names bighash *]]

    test {HVALS - small hash} {
        set vals {}
        foreach {k v} [array get smallhash] {
            lappend vals $v
        }
        set _ [lsort $vals]
    } [lsort [r hvals smallhash]]

    test {HVALS - big hash} {
        set vals {}
        foreach {k v} [array get bighash] {
            lappend vals $v
        }
        set _ [lsort $vals]
    } [lsort [r hvals bighash]]

    test {HGETALL - small hash} {
        lsort [r hgetall smallhash]
    } [lsort [array get smallhash]]

    test {HGETALL - big hash} {
        lsort [r hgetall bighash]
    } [lsort [array get bighash]]

    test {HDEL and return value} {
        set rv {}
        lappend rv [r hdel smallhash nokey]
        lappend rv [r hdel bighash nokey]
        set k [lindex [array names smallhash *] 0]
        lappend rv [r hdel smallhash $k]
        lappend rv [r hdel smallhash $k]
        lappend rv [r hget smallhash $k]
        unset smallhash($k)
        set k [lindex [array names bighash *] 0]
        lappend rv [r hdel bighash $k]
        lappend rv [r hdel bighash $k]
        lappend rv [r hget bighash $k]
        unset bighash($k)
        set _ $rv
    } {0 0 1 0 {} 1 0 {}}

    test {HDEL - more than a single value} {
        set rv {}
        r del myhash
        r hmset myhash a 1 b 2 c 3
        assert_equal 0 [r hdel myhash x y]
        assert_equal 2 [r hdel myhash a c f]
        r hgetall myhash
    } {b 2}

    test {HDEL - hash becomes empty before deleting all specified fields} {
        r del myhash
        r hmset myhash a 1 b 2 c 3
        assert_equal 3 [r hdel myhash a b c d e]
        assert_equal 0 [r exists myhash]
    }

    test {HEXISTS} {
        set rv {}
        set k [lindex [array names smallhash *] 0]
        lappend rv [r hexists smallhash $k]
        lappend rv [r hexists smallhash nokey]
        set k [lindex [array names bighash *] 0]
        lappend rv [r hexists bighash $k]
        lappend rv [r hexists bighash nokey]
    } {1 0 1 0}

    test {Is a ziplist encoded Hash promoted on big payload?} {
        r hset smallhash foo [string repeat a 1024]
        #debug object does not work
        #r debug object smallhash
    } {*hashtable*}

    test {HINCRBY against non existing database key} {
        r del htest
        list [r hincrby htest foo 2]
    } {2}

    test {HINCRBY against non existing hash key} {
        set rv {}
        r hdel smallhash tmp
        r hdel bighash tmp
        lappend rv [r hincrby smallhash tmp 2]
        lappend rv [r hget smallhash tmp]
        lappend rv [r hincrby bighash tmp 2]
        lappend rv [r hget bighash tmp]
    } {2 2 2 2}

    test {HINCRBY against hash key created by hincrby itself} {
        set rv {}
        lappend rv [r hincrby smallhash tmp 3]
        lappend rv [r hget smallhash tmp]
        lappend rv [r hincrby bighash tmp 3]
        lappend rv [r hget bighash tmp]
    } {5 5 5 5}

    test {HINCRBY against hash key originally set with HSET} {
        r hset smallhash tmp 100
        r hset bighash tmp 100
        list [r hincrby smallhash tmp 2] [r hincrby bighash tmp 2]
    } {102 102}

    test {HINCRBY over 32bit value} {
        r hset smallhash tmp 17179869184
        r hset bighash tmp 17179869184
        list [r hincrby smallhash tmp 1] [r hincrby bighash tmp 1]
    } {17179869185 17179869185}

    test {HINCRBY over 32bit value with over 32bit increment} {
        r hset smallhash tmp 17179869184
        r hset bighash tmp 17179869184
        list [r hincrby smallhash tmp 17179869184] [r hincrby bighash tmp 17179869184]
    } {34359738368 34359738368}

    test {HINCRBY fails against hash value with spaces (left)} {
        r hset smallhash str " 11"
        r hset bighash str " 11"
        catch {r hincrby smallhash str 1} smallerr
        catch {r hincrby smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not an integer*" $smallerr]
        lappend rv [string match "ERR*not an integer*" $bigerr]
    } {1 1}

    test {HINCRBY fails against hash value with spaces (right)} {
        r hset smallhash str "11 "
        r hset bighash str "11 "
        catch {r hincrby smallhash str 1} smallerr
        catch {r hincrby smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not an integer*" $smallerr]
        lappend rv [string match "ERR*not an integer*" $bigerr]
    } {1 1}

    test {HINCRBY can detect overflows} {
        set e {}
        r hset hash n -9223372036854775484
        assert {[r hincrby hash n -1] == -9223372036854775485}
        catch {r hincrby hash n -10000} e
        set e
    } {*overflow*}

	test {HINCRBY 1.2 bug: http://tapd.oa.com/IEG_Redis_Cluster/bugtrace/bugs/view?bug_id=1010095231053705891 } {
   		r del hash
   		r hset hash a 0
   		r hset hash b 0
   		r hincrby hash a 1
   		r del hash
   		r hlen hash
    } {0}

    test {HINCRBYFLOAT against non existing database key} {
        r del htest
        list [r hincrbyfloat htest foo 2.5]
    } {2.5}

    test {HINCRBYFLOAT against non existing hash key} {
        set rv {}
        r hdel smallhash tmp
        r hdel bighash tmp
        lappend rv [roundFloat [r hincrbyfloat smallhash tmp 2.5]]
        lappend rv [roundFloat [r hget smallhash tmp]]
        lappend rv [roundFloat [r hincrbyfloat bighash tmp 2.5]]
        lappend rv [roundFloat [r hget bighash tmp]]
    } {2.5 2.5 2.5 2.5}

    test {HINCRBYFLOAT against hash key created by hincrby itself} {
        set rv {}
        lappend rv [roundFloat [r hincrbyfloat smallhash tmp 3.5]]
        lappend rv [roundFloat [r hget smallhash tmp]]
        lappend rv [roundFloat [r hincrbyfloat bighash tmp 3.5]]
        lappend rv [roundFloat [r hget bighash tmp]]
    } {6 6 6 6}

    test {HINCRBYFLOAT against hash key originally set with HSET} {
        r hset smallhash tmp 100
        r hset bighash tmp 100
        list [roundFloat [r hincrbyfloat smallhash tmp 2.5]] \
             [roundFloat [r hincrbyfloat bighash tmp 2.5]]
    } {102.5 102.5}

    test {HINCRBYFLOAT over 32bit value} {
        r hset smallhash tmp 17179869184
        r hset bighash tmp 17179869184
        list [r hincrbyfloat smallhash tmp 1] \
             [r hincrbyfloat bighash tmp 1]
    } {17179869185 17179869185}

    test {HINCRBYFLOAT over 32bit value with over 32bit increment} {
        r hset smallhash tmp 17179869184
        r hset bighash tmp 17179869184
        list [r hincrbyfloat smallhash tmp 17179869184] \
             [r hincrbyfloat bighash tmp 17179869184]
    } {34359738368 34359738368}

    test {HINCRBYFLOAT fails against hash value with spaces (left)} {
        r hset smallhash str " 11"
        r hset bighash str " 11"
        catch {r hincrbyfloat smallhash str 1} smallerr
        catch {r hincrbyfloat smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not*float*" $smallerr]
        lappend rv [string match "ERR*not*float*" $bigerr]
    } {1 1}

    test {HINCRBYFLOAT fails against hash value with spaces (right)} {
        r hset smallhash str "11 "
        r hset bighash str "11 "
        catch {r hincrbyfloat smallhash str 1} smallerr
        catch {r hincrbyfloat smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not*float*" $smallerr]
        lappend rv [string match "ERR*not*float*" $bigerr]
    } {1 1}

    test {Hash ziplist regression test for large keys} {
        r hset hash kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk a
        r hset hash kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk b
        r hget hash kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk
    } {b}

    foreach size {10 512} {
        test "Hash fuzzing #1 - $size fields" {
            for {set times 0} {$times < 10} {incr times} {
                catch {unset hash}
                array set hash {}
                r del hash
                # Create
                for {set j 0} {$j < $size} {incr j} {
                    set field [randomValue]
                    set value [randomValue]
                    r hset hash $field $value
                    set hash($field) $value
                }

                # Verify
                foreach {k v} [array get hash] {
                    assert_equal $v [r hget hash $k]
                }
                assert_equal [array size hash] [r hlen hash]
            }
        }

        test "Hash fuzzing #2 - $size fields" {
            for {set times 0} {$times < 10} {incr times} {
                catch {unset hash}
                array set hash {}
                r del hash
                # Create
                for {set j 0} {$j < $size} {incr j} {
                    randpath {
                        set field [randomValue]
                        set value [randomValue]
                        r hset hash $field $value
                        set hash($field) $value
                    } {
                        set field [randomSignedInt 512]
                        set value [randomSignedInt 512]
                        r hset hash $field $value
                        set hash($field) $value
                    } {
                        randpath {
                            set field [randomValue]
                        } {
                            set field [randomSignedInt 512]
                        }
                        r hdel hash $field
                        unset -nocomplain hash($field)
                    }
                }

                # Verify
                foreach {k v} [array get hash] {
                    assert_equal $v [r hget hash $k]
                }
                assert_equal [array size hash] [r hlen hash]
            }
        }
    }

    test {Stress test the hash ziplist -> hashtable encoding conversion} {
        r config set hash-max-ziplist-entries 32
        for {set j 0} {$j < 100} {incr j} {
            r del myhash
            for {set i 0} {$i < 64} {incr i} {
                r hset myhash [randomValue] [randomValue]
            }
            #assert {[r object encoding myhash] eq {hashtable}}
        }
    }

    test "hmcas and hmgetvsn basic" {
    	r del hash
    	r hmcas hash 0 1000 a 0 b b 0 c 1 0 100
   		r hmgetvsn hash a b 1
    } {1000 b c 100}

    test "hmcas and del" {
    	r del hash
   		r hmcas hash 0 1000 a 0 b b 0 c 1 0 100
   		r hmcas hash 1 1000 a 0 b b 0 c 1 0 100
   		assert_equal [r hlen hash] {3}
   		r del hash
   		assert_equal [r hlen hash] {0}
    } {}

    test "hmcas duplicate key 1" {
   		r hmcas hash 0 1000 a 0 b a 0 100 1 0 100
   		r hmgetvsn hash a 1
    } {1000 100 100}

    test "hmcas duplicate key 2" {
   		r hmcas hash 0 199 a 0 b 1 0 100
   		r hmcas hash 0 200 a 0 c 100 0 1
   		r hmgetvsn hash a 1 100
    } {200 c 100 1}

    test "hmcas refuse incorrect version" {
    	r del hash
   		r hmcas hash 0 199 1 0 2
   		r hmcas hash 1 188 2 0 3
    } {0}

    test "hmcas increase version" {
   		r hmcas hash 0 199 1 0 2
   		r hmcas hash 1 199 a 0 b
   		r hmgetvsn hash 1 a
    } {200 2 b}

    test "mix hmcas and hmset: hmset erase the version " {
   		r del hash
   		r hmcas hash 0 199 1 0 2 3 0 4 5 0 6
   		r hmset hash 1 1 3 3 5 5
   		assert_equal [r hvals hash] {1 3 5}
   		r hmgetvsn hash 1 3 5
    } {-1 1 3 5}

    test "mix hmset and hmcas: hmcas can force version a hash" {
   		r del hash
   		r hmset hash 1 2 3 4 5 6
   		r hmcas hash 0 199 1 0 1 3 0 3 5 0 5
   		r hmgetvsn hash 1 3 5
    } {199 1 3 5}

   	test "mix hmset and hmcas: hmcas failed on an unversion hash" {
   		r del hash
   		r hmset hash 1 2 3 4 5 6
   		r hmcas hash 1 199 1 0 1 3 0 3 5 0 5
      r hmgetvsn hash 1
   	} {199 1}

   	test "hmget nonexisting key: vsn 0 and all keys nil" {
   		r del hash
   		r hmgetvsn hash 1 2 3 4 5
   	} {{} {} {} {} {} {}}

  	test "hmcasv2 basic: return 0 on comparision failure" {
  		r del hash
  		r hmcasv2 hash 0 100 199 a 0 b
  		r hmcasv2 hash 1 188 200 a 0 b
  	} {0}

	test "hmcasv2 basic: return 1 on comparision success" {
  		r del hash
  		r hmcasv2 hash 0 100 199 a 0 b
  		r hmcasv2 hash 1 199 200 a 0 b
  	} {1}

  	test "hmcasv2 basic: force version" {
  		r del hash
  		r hmcasv2 hash 0 100 199 a 0 b
  		r hmgetvsn hash a
  	} {199 b}

	test "hmcasv2: use provided new version" {
		r del hash
		r hmcasv2 hash 0 100 199 a 0 b
		r hmcasv2 hash 1 199 999 a 0 c
		r hmgetvsn hash a
	} {999 c}

	test "hmcas and hmcasv2" {
		r del hash
		r hmcas hash 0 199 a 0 b
		r hmcasv2 hash 0 200 250 a 0 100
		r hmgetvsn hash a
	} {250 100}

  test "hmcas cmp be 0 or 1" {
    r del hash
    assert_error "*cmp should be 0 or 1*" {r hmcas hash -1 199 a 0 b}
  }

  test "hmcas vsn could be negative when insert" {
    r del hash
    r hmcas hash 0 -100 a 0 d
    r hmgetvsn hash a
  } {-100 d}

  test "hmcas with campare on non-existing key set version to version in command" {
    r del hash
    r hmcas hash 1 -100 a 0 d
    r hmgetvsn hash a
  }  {-100 d}

  test "hmcasv2 cmp be 0 or 1" {
    r del hash
    assert_error "*cmp should be 0 or 1*" {r hmcasv2 hash -1 1 199 a 0 b}
  }

  test "hmcasv2 vsn newvsn not equal" {
    r del hash
    assert_error "*new version must be greater than old version*" {r hmcasv2 hash 1 199 199 a 0 b}
  }

  test "hmcas negative version" {
    r del hash
    r hmcas hash 0 -1 a 0 b
    r hmgetvsn hash a
  } {-1 b}

  test "hmcasv2 version increase: new data version can be negative when update" {
    r del hash
    r hmcasv2 hash 0 -1 -10 a 0 b
    r hmgetvsn hash a
  } {-10 b}

  test "hmcasv2 version increase: new data version must be greater when update" {
    r del hash
    r hmcasv2 hash 0 -1 199 a 0 c
    assert_error "*new version must be greater than*" {r hmcasv2 hash 1 199 -100 a 0 d}
  }

  test "hmcasv2 version increase: new data version cannot be smaller" {
    r del hash
    r hmcasv2 hash 0 -1 199 a 0 c
    assert_error "*new version must be greater than*" {r hmcasv2 hash 1 199 100 a 0 d}
  }

  test "hmcasv2 add operator: wrong number of arguments" {
    r del hash
    assert_error "*wrong number of arguments*" {r hmcasv2 hash 1 199 100 a d}
  }

  test "hmcasv2 add operator: wrong opcode" {
    r del hash
    assert_error "*ERR*" {r hmcasv2 hash 0 199 100 a -1 d}
    assert_error "*ERR*" {r hmcasv2 hash 0 199 100 a 2 d}
    assert_error "*ERR*" {r hmcasv2 hash 0 199 100 a 100 d}
  }

  test "hmcasv2 add operator: non-existing hash" {
    r del hash
    r hmcasv2 hash 0 -1 1000 a 1 100
    r hmcasv2 hash 1 1000 1000000 a 1 500
    r hmgetvsn hash a
  } {1000000 600}

  test "hmcasv2 add operator: increase value" {
    r del hash
    r hmcasv2 hash 0 -1 1000 a 1 -100
    r hmcasv2 hash 1 1000 10001 a 1 199
    r hmgetvsn hash a
  } {10001 99}

  test "hmcasv2 add operator: wrong data type string" {
    r del hash
    r hmcasv2 hash 0 -1 1000 a 0 stringtype
    assert_error "*ERR*" {r hmcasv2 hash 1 1000 a 1 100}
  }

  test "hmcasv2 add operator: wrong data type float" {
    r del hash
    r hmcasv2 hash 0 -1 1000 a 0 0.111
    assert_error "*ERR*" {r hmcasv2 hash 1 1000 a 1 100}
  }

  test "hmcasv2 with compare on non-existing key" {
    r del hash
    r hmcasv2 hash 1 -200 1000 a 0 0.111
    r hmgetvsn hash a
  }  {1000 0.111}

  test "hmcasv2 with compare on non-existing key" {
    r del hash
    r hmcasv2 hash 1 -2000 1000 a 0 0.111
    r hmgetvsn hash a
  }  {1000 0.111}

  test "hmcas mix hmcasv2" {
    r del hash
    r hmcas hash 0 1000 a 0 1111
    r hmcasv2 hash 1 1000 1009 a 1 -445
    r hmgetvsn hash a
  } {1009 666}

  test "hmcas misc" {
    r del hash
    r hmcas hash 0 0 a 0 1111
    r hmcasv2 hash 1 0 2 a 0 stringtype
    r hmcasv2 hash 1 2 3 a 0 1
    r hmcas hash 1 3 a 1 1023
    r hmcasv2 hash 0 -1 999 a 1 1024
    r hmgetvsn hash a
  }  {999 2048}

  test "hmcas works in sync" {
    r del hash
    r hmcasv2 hash 0 -1 1009 a 0 -445
    r hmcasv2 hash 1 1009 1010 a 0 8888

    start_server {} {
      r flushalldisk
      r slaveof [srv -1 host] [srv -1 port]
      after 5000
      assert_equal [r 0 hmgetvsn hash a] [r -1 hmgetvsn hash a]

      r -1 hmcasv2 hash 1 1010 1088 b 0 fuckbbbbbbbbb
      after 1000
      assert_equal [r 0 hmgetvsn hash a] [r -1 hmgetvsn hash a]
      assert_equal [r hmgetvsn hash a b] {1088 8888 fuckbbbbbbbbb}
    }
  }

  test "hmcas and expire" {
    r del hash
    r hmcasv2 hash 0 -1 1009 a 0 -445
    r expire hash 10
    after 10000
    r hmgetvsn hash a
  } {{} {}}

  test "hmcas and expire works in sync" {
    r del hash
    r hmcasv2 hash 0 -1 1009 a 0 -445
    r hmcasv2 hash 1 1009 1010 a 0 8888

    start_server {} {
      r flushalldisk
      r slaveof [srv -1 host] [srv -1 port]
      after 5000
      assert_equal [r 0 hmgetvsn hash a] [r -1 hmgetvsn hash a]

      r -1 hmcasv2 hash 1 1010 1088 b 0 fuckbbbbbbbbb
      after 1000
      assert_equal [r 0 hmgetvsn hash a] [r -1 hmgetvsn hash a]
      assert_equal [r hmgetvsn hash a b] {1088 8888 fuckbbbbbbbbb}

      r -1 expire hash 10
      after 10000
      assert_equal {{} {}} [r hmgetvsn hash a]
    }
  }

  test "hmcasv2 version-increase 0" {
    start_server {config nondefault.conf} {
      test "start to test" {
        r flushalldisk

        r hmcasv2 hash 0 -300 -500 fuck 0 you
        r hmcasv2 hash 1 -500 -1088 fuck 0 fuckbbbbbbbbb
        r hmgetvsn hash fuck
      } {-1088 fuckbbbbbbbbb}
    }
  }
}
