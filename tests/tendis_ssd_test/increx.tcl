start_server {tags {"cas"}} { 
    test {INCREX against non existing key} {
        r del novar
        set res {}
        append res [r increx novar 5]
        append res [r get novar]
        append res [r ttl novar]
    } {115}

    test {INCREX against key created with ttl by increx itself} {
        set res {}
        append res [r increx novar 100]
        append res [r ttl novar]
    } {25}

    test {INCREX against key originally set with SET} {
        set res {}
        r set novar 100
        append res [r increx novar 200]
        append res [r ttl novar]
    } {101200}

    test {INCREX over a key with a ttl} {
        r setex novar 200 17179869184
        r increx novar 300
        set ttl [r ttl novar]
        assert { $ttl <= 200 }
        r get novar
    } {17179869185}
}