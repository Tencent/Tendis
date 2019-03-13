start_server {tags {"cas"}} {
    test {cas on non existing key} {
      r del caskey
      r cas caskey 0 10
      r getvsn caskey
    } {0 10}

    test "cas on existing unversioned key" {
      r del unversionedkey
      r set unversionedkey 100
      r cas unversionedkey 100 200
      r getvsn unversionedkey
    } {100 200}

    test {cas version matches} {
      r del versionedkey
      r cas versionedkey 1000 b
      r cas versionedkey 1000 c
      r getvsn versionedkey
    } {1001 c}

    test {set removes version} {
      r del versionedkey
      r cas versionedkey 1 a
      r cas versionedkey 1 b
      r set versionedkey c
      r getvsn versionedkey
    } {-1 c}

    test "cas version mismatches" {
      r del versionedkey
      r cas versionedkey 1000 b
      assert_error "*vsn mismatch or cas on an unversioned key*" {r cas versionedkey 1 c}
    }

    test {getvsn unversioned key} {
      r del unversionedkey
      r set unversionedkey 888
      r getvsn unversionedkey
    } {-1 888}

    test {getvsn non existing key} {
      r del nonexistingkey
      r getvsn nonexistingkey
    } {-1 {}}

    test {get on versioned key} {
      r del versionedkey
      r cas versionedkey 1 a
      r get versionedkey
    } {a}

    test {cas set version on non-existing key} {
      r del nonkey
      r cas nonkey 100 a
      r getvsn nonkey
    } {100 a}

    start_server {} {
      test {cas worked with sync} {
        r -1 del a
        r -1 cas a 100 bbbbbbbbb

        r flushalldisk
        r slaveof [srv -1 host] [srv -1 port]
        after 10000
        assert_equal [r 0 getvsn a] [r -1 getvsn a]

        r -1 cas a 100 8888888
        after 5000
        assert_equal [r 0 getvsn a] [r -1 getvsn a]
        r -1 cas a 101 888888888xxxxxxx
        assert_equal {102 888888888xxxxxxx} [r -1 getvsn a]
      }
    }
}
