start_server {tags {"dump"}} {
    test {DUMP / RESTORE are able to serialize / unserialize a simple key} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        list [r exists foo] [r restore foo 0 $encoded] [r ttl foo] [r get foo]
    } {0 OK -1 bar}

    test {RESTORE can set an arbitrary expire to the materialized key} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        r restore foo 5000 $encoded
        set ttl [r pttl foo]
        assert {$ttl >= 3000 && $ttl <= 5000}
        r get foo
    } {bar}

    test {RESTORE can set an expire that overflows a 32 bit integer} {
        r set foo bar
        set encoded [r dump foo]
        r del foo
        r restore foo 2569591501 $encoded
        set ttl [r pttl foo]
        assert {$ttl >= (2569591501-3000) && $ttl <= 2569591501}
        r get foo
    } {bar}

    test {RESTORE returns an error of the key already exists} {
        r set foo bar
        set e {}
        catch {r restore foo 0 "..."} e
        set e
    } {*BUSYKEY*}

    # TODO(comboqiu): not implement REPLACE
    #test {RESTORE can overwrite an existing key with REPLACE} {
    #    r set foo bar1
    #    set encoded1 [r dump foo]
    #    r set foo bar2
    #    set encoded2 [r dump foo]
    #    r del foo
    #    r restore foo 0 $encoded1
    #    r restore foo 0 $encoded2 replace
    #    r get foo
    #} {bar2}

    test {RESTORE can detect a syntax error for unrecongized options} {
        catch {r restore foo 0 "..." invalid-option} e
        set e
    } {*syntax*}

    test {DUMP of non existing key returns nil} {
        r dump nonexisting_key
    } {}

}
