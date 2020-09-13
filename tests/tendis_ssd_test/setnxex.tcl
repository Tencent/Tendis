start_server {tags {"setnxex"}} {
    test "SETNXEX target key missing" {
        r del novar
        assert_equal 1 [r setnxex novar 100 foobared]
        assert_equal "foobared" [r get novar]
    }

    test "SETNXEX target key exists" {
        r set novar foobared
        assert_equal 0 [r setnxex novar 1000 blabla]
        assert_equal "foobared" [r get novar]
    }

    test "SETNXEX target key ttl" {
        r del novar
        assert_equal 1 [r setnxex novar 3 foobared]
        after 5000
        set ttl [r ttl novar]
        r get novar
    } {}

    test "SETNXEX against not-expired volatile key" {
        r set x 10
        r expire x 10000
        assert_equal 0 [r setnxex x 1000 20]
        assert_equal 10 [r get x]
    }

    test "SETNXEX against expired volatile key" {
        # Make it very unlikely for the key this test uses to be expired by the
        # active expiry cycle. This is tightly coupled to the implementation of
        # active expiry and dbAdd() but currently the only way to test that
        # SETNX expires a key when it should have been.
        for {set x 0} {$x < 9999} {incr x} {
            r setex key-$x 3600 value
        }

        # This will be one of 10000 expiring keys. A cycle is executed every
        # 100ms, sampling 10 keys for being expired or not.  This key will be
        # expired for at most 1s when we wait 2s, resulting in a total sample
        # of 100 keys. The probability of the success of this test being a
        # false positive is therefore approx. 1%.
        r set x 10
        r expire x 1
        # Wait for the key to expire
        after 2000

        assert_equal 1 [r setnxex x 1000 20]
        assert_equal 20 [r get x]
    }
}