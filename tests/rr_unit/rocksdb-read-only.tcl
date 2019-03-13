start_server {tags {"expire"} "rocksdb-read-only"} {
  test {rocksdb-read-only works} {
    assert_error "*ROCKSDBREAONLY You can't write against a rocksdb read only instance*" {r set a b}
  }

  test {set rocksdb-read-only from yes to no} {
    r config set rocksdb-read-only no
    r set a b
    r get a
  } {b}

  test {set rocksdb-read-only from no to yes not allowed} {
    assert_error "*ERR not allowed to change rocksdb-read-only from 'yes' to 'no' via config set*" {r config set rocksdb-read-only yes}
  }

  start_server {tags {"rocksdb-read-only"} "rocksdb-read-only"} {
    test {reset rocksdb-read-only in master-slave-1} {
      assert_equal "rocksdb-read-only no" [r -1 config get rocksdb-read-only]
      assert_equal "rocksdb-read-only yes" [r config get rocksdb-read-only]
      r -1 set a bbbbbbbbbbb
      r slaveof [srv -1 host] [srv -1 port]
      after 3000
      assert_equal "bbbbbbbbbbb" [r get a]

      assert_equal "rocksdb-read-only yes" [r config get rocksdb-read-only]
      assert_equal "slave-read-only yes" [r config get slave-read-only]
      r config set slave-read-only no
      assert_equal "slave-read-only no" [r config get slave-read-only]
      r hset heros h1 ultraman
      assert_equal "ultraman" [r hget heros h1]

      r slaveof no one
      assert_equal "rocksdb-read-only no" [r config get rocksdb-read-only]
      r set c d
      assert_equal "d" [r get c]
    }

    test {reset rocksdb-read-only in master-slave-2} {
      r config set slave-read-only yes

      assert_equal "rocksdb-read-only no" [r -1 config get rocksdb-read-only]
      assert_equal "rocksdb-read-only no" [r config get rocksdb-read-only]
      r -1 set a bbbbbbbbbbb
      r slaveof [srv -1 host] [srv -1 port]
      after 3000
      assert_equal "bbbbbbbbbbb" [r get a]

      assert_equal "rocksdb-read-only no" [r config get rocksdb-read-only]
      assert_equal "slave-read-only yes" [r config get slave-read-only]
      assert_error "*READONLY You can't write against a read only slave*" {r hset heros h1 ultraman}

      r slaveof no one
      assert_equal "rocksdb-read-only no" [r config get rocksdb-read-only]
      r set c d
      assert_equal "d" [r get c]
    }
  }
}
