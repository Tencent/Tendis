start_server {tags {"hmcas"}} { 
    test "hmcas and hmgetvsn basic" {
    	r del hash
    	r hmcas hash 0 1000 a 0 b b 0 c 1 0 100
   		r hmgetvsn hash a b 1
    } {1001 b c 100}

    test "hmcas and del" {
    		r del hash
   		r hmcas hash 0 1000 a 0 b b 0 c 1 0 100
   		r hmcas hash 1 1000 a 0 b b 0 c 1 0 100
   		assert_equal [r hlen hash] {3}
   		r del hash
   		assert_equal [r hlen hash] {0}
    } {}

    test "hmcas duplicate key 1" {
		r del hash
   		r hmcas hash 0 1000 a 0 b a 0 100 1 0 100
   		r hmgetvsn hash a 1
    } {1001 100 100}

    test "hmcas duplicate key 2" {
		r del hash
   		r hmcas hash 0 199 a 0 b 1 0 100
   		r hmcas hash 0 200 a 0 c 100 0 1
   		r hmgetvsn hash a 1 100
    } {201 c 100 1}

    test "hmcas refuse incorrect version" {
    	r del hash
   		r hmcas hash 0 199 1 0 2
   		r hmcas hash 1 188 2 0 3
    } {0}

    test "hmcas increase version" {
		r del hash
   		r hmcas hash 0 199 1 0 2
   		r hmcas hash 1 199 a 0 b
   		r hmgetvsn hash 1 a
    } {200 2 {}}

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
    } {200 1 3 5}

   	test "mix hmset and hmcas: hmcas failed on an unversion hash" {
   		r del hash
   		r hmset hash 1 2 3 4 5 6
   		r hmcas hash 1 199 1 0 1 3 0 3 5 0 5
      r hmgetvsn hash 1
   	} {200 1}

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
  } {-99 d}

  test "hmcas with campare on non-existing key set version to version in command" {
    r del hash
    r hmcas hash 1 -100 a 0 d
    r hmgetvsn hash a
  }  {-99 d}

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
  } {0 b}

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
  } {1001 1111}

  test "hmcas misc" {
    r del hash
    r hmcas hash 0 0 a 0 1111
    r hmcasv2 hash 1 0 2 a 0 stringtype
    r hmcasv2 hash 1 2 3 a 0 1
    r hmcas hash 1 3 a 1 1023
    r hmcasv2 hash 0 -1 999 a 1 1024
    r hmgetvsn hash a
  }  {999 2135}

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
      after 3000
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
    start_server {config tendisplus.nodefault.conf} {
      test "start to test" {
        r flushalldisk

        r hmcasv2 hash 0 -300 -500 fuck 0 you
        r hmcasv2 hash 1 -500 -1088 fuck 0 fuckbbbbbbbbb
        r hmgetvsn hash fuck
      } {-1088 fuckbbbbbbbbb}
    }
  }
}