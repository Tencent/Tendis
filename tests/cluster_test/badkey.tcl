start_server {
    tags ["badkey"]
} {
    r flushall

    test "append"       {assert_equal 1     [r append not_exists_k0 v]}
    test "decr"         {assert_equal -1    [r decr not_exists_k1]}
    test "decrby"       {assert_equal -1    [r decrby not_exists_k2 1]}
    test "get"          {assert_equal ""    [r get not_exists_k3]}
    test "getrange"     {assert_equal ""    [r getrange not_exists_k4 0 1]}
    test "getset"       {assert_equal ""    [r getset not_exists_k5 v]}
    test "incr"         {assert_equal 1     [r incr not_exists_k6]}
    test "incrby"       {assert_equal 1     [r incrby not_exists_k7 1]}
    test "incrbyfloat"  {assert_equal "1"   [r incrbyfloat not_exists_k8 1]}
    test "mget"         {assert_equal "{}"  [r mget not_exists_k9]}
    test "mset"         {assert_equal OK    [r mset not_exists_k10 v]}
    test "msetnx"       {assert_equal 1     [r msetnx not_exists_k11 v]}
    test "psetex"       {assert_equal OK    [r psetex not_exists_k12 1 v]}
    test "set"          {assert_equal OK    [r set not_exists_k13 v]}
    test "setex"        {assert_equal OK    [r setex not_exists_k14 1 v]}
    test "setnx"        {assert_equal 1     [r setnx not_exists_k15 v]}
    test "setrange"     {assert_equal 1     [r setrange not_exists_k16 0 v]}
    test "strlen"       {assert_equal 0     [r strlen not_exists_k17]}
    test "substr"       {assert_equal ""    [r substr not_exists_k18 0 1]}

    test "hdel"         {assert_equal 0         [r hdel not_exists_k19 field]}
    test "hexists"      {assert_equal 0         [r hexists not_exists_k20 field]}
    test "hget"         {assert_equal ""        [r hget not_exists_k21 field]}
    test "hgetall"      {assert_equal ""        [r hgetall not_exists_k22]}
    test "hincrby"      {assert_equal 1         [r hincrby not_exists_k23 field 1]}
    test "hincrbyfloat" {assert_equal "1"       [r hincrbyfloat not_exists_k24 field 1]}
    test "hkeys"        {assert_equal ""        [r hkeys not_exists_k25]}
    test "hlen"         {assert_equal 0         [r hlen not_exists_k26]}
    test "hmegt"        {assert_equal "{}"      [r hmget not_exists_k27 field]}
    test "hmset"        {assert_equal OK        [r hmset not_exists_k28 field v]}
    test "hset"         {assert_equal 1         [r hset not_exists_k29 field v]}
    test "hsetnx"       {assert_equal 1         [r hsetnx not_exists_k30 field v]}
    test "hstrlen"      {assert_equal 0         [r hstrlen not_exists_k31 field]}
    test "hvals"        {assert_equal ""        [r hvals not_exists_k32]}
    test "hscan"        {assert_equal "0 {}"    [r hscan not_exists_k33 0]}

    test "lindex"       {assert_equal ""                [r lindex not_exists_k34 0]}
    test "linsert"      {assert_equal 0                 [r linsert not_exists_k35 before p v]}
    test "llen"         {assert_equal 0                 [r llen not_exists_k36]}
    test "lpop"         {assert_equal ""                [r lpop not_exists_k37]}
    test "lpush"        {assert_equal 1                 [r lpush not_exists_k38 v]}
    test "lpushx"       {assert_equal 0                 [r lpushx not_exists_k39 v]}
    test "lrange"       {assert_equal ""                [r lrange not_exists_k40 0 1]}
    test "lrem"         {assert_equal 0                 [r lrem not_exists_k41 0 v]}
    # test "lset"         {assert_equal "ERR no such key" [r lset not_exists_k42 0 v]}
    test "ltrim"        {assert_equal OK                [r ltrim not_exists_k43 0 1]}
    test "rpop"         {assert_equal ""                [r rpop not_exists_k44]}
    test "rpoplpush"    {assert_equal ""                [r rpoplpush not_exists_k45 not_exists_k46]}
    test "rpush"        {assert_equal 1                 [r rpush not_exists_k47 v]}
    test "rpushx"       {assert_equal 0                 [r rpushx not_exists_k48 v]}

    test "sadd"         {assert_equal 1         [r sadd not_exists_k49 m]}
    test "scard"        {assert_equal 0         [r scard not_exists_k50]}
    test "sdiff"        {assert_equal ""        [r sdiff not_exists_k51]}
    test "sdiffstore"   {assert_equal 0         [r sdiffstore not_exists_k52 not_exists_k53]}
    test "sinter"       {assert_equal ""        [r sinter not_exists_k54]}
    test "sinterstore"  {assert_equal 0         [r sinterstore not_exists_k55 not_exists_k56]}
    test "sismember"    {assert_equal 0         [r sismember not_exists_k57 m]}
    test "smembers"     {assert_equal ""        [r smembers not_exists_k58]}
    test "smove"        {assert_equal 0         [r smove not_exists_k59 not_exists_k60 member]}
    test "spop"         {assert_equal ""        [r spop not_exists_k61]}
    test "srandmember"  {assert_equal ""        [r srandmember not_exists_k62]}
    test "srem"         {assert_equal 0         [r srem not_exists_k63 member]}
    test "sscan"        {assert_equal "0 {}"    [r sscan not_exists_k64 0]}
    test "sunion"       {assert_equal ""        [r sunion not_exists_k65]}
    test "sunionstore"  {assert_equal 0         [r sunionstore not_exists_k66 not_exists_k67]}

    test "zadd"             {assert_equal 1         [r zadd not_exists_k68 0 member]}
    test "zcard"            {assert_equal 0         [r zcard not_exists_k69]}
    test "zcount"           {assert_equal 0         [r zcount not_exists_k70 0 1]}
    test "zincrby"          {assert_equal "1"       [r zincrby not_exists_k71 1 member]}
    test "zinterstore"      {assert_equal 0         [r zinterstore not_exists_72 1 not_exists_k73]}
    test "zlexcount"        {assert_equal 0         [r zlexcount not_exists_k74 - +]}
    test "zrange"           {assert_equal ""        [r zrange not_exists_k75 0 1]}
    test "zrangebylex"      {assert_equal ""        [r zrangebylex not_exists_k76 - +]}
    test "zrangebyscore"    {assert_equal ""        [r zrangebyscore not_exists_k77 0 1]}
    test "zrank"            {assert_equal ""        [r zrank not_exists_k78 member]}
    test "zrem"             {assert_equal 0         [r zrem not_exists_k79 member]}
    test "zremrangebylex"   {assert_equal 0         [r zremrangebylex not_exists_k80 - +]}
    test "zremrangebyrank"  {assert_equal 0         [r zremrangebyrank not_exists_k81 0 1]}
    test "zremrangebyscore" {assert_equal 0         [r zremrangebyscore not_exists_k82 0 1]}
    test "zrevrange"        {assert_equal ""        [r zrevrange not_exists_k83 0 1]}
    test "zrevrangebylex"   {assert_equal ""        [r zrevrangebylex not_exists_k84 + -]}
    test "zrevrangebyscore" {assert_equal ""        [r zrevrangebyscore not_exists_k85 1 0]}
    test "zscan"            {assert_equal "0 {}"    [r zscan not_exists_k86 0]}
    test "zscore"           {assert_equal ""        [r zscore not_exists_k87 member]}
    test "zunionstore"      {assert_equal 0         [r zunionstore not_exists_k88 1 not_exists_k89]}

    test "del"              {assert_equal 0                 [r del not_exists_k90]}
    test "dump"             {assert_equal ""                [r dump not_exists_k91]}
    test "exists"           {assert_equal 0                 [r exists not_exists_k92]}
    test "expire"           {assert_equal 0                 [r expire not_exists_k93 1]}
    test "expireat"         {assert_equal 0                 [r expireat not_exists_k94 1]}
    test "persist"          {assert_equal 0                 [r persist not_exists_k95]}
    test "pexpire"          {assert_equal 0                 [r pexpire not_exists_k96 1]}
    test "pexpireat"        {assert_equal 0                 [r pexpireat not_exists_k97 1]}
    test "pttl"             {assert_equal -2                [r pttl not_exists_k98]}
    test "sort"             {assert_equal ""                [r sort not_exists_k99]}
    # test "rename"           {assert_equal "ERR no such key" [r rename not_exists_k100 key]}
    # test "renamenx"         {assert_equal "ERR no such key" [r renamenx not_exists_k101 key]}
    # test "restore"          {assert_equal "ERR DUMP payload version or checksum are wrong" [r restore not_exists_k102 0 v]}
    test "ttl"              {assert_equal -2                [r ttl not_exists_k103]}
    test "type"             {assert_equal none              [r type not_exists_k104]}
    test "unlink"           {assert_equal 0                 [r unlink not_exists_k105]}

    test "getbit"           {assert_equal 0  [r getbit not_exists_k106 0]}
    test "setbit"           {assert_equal 0  [r setbit not_exists_k107 0 1]}
    test "bitcount"         {assert_equal 0  [r bitcount not_exists_k108]}
    test "bitpos"           {assert_equal 0  [r bitpos not_exists_k109 0]}
    test "bitfield"         {assert_equal "" [r bitfield not_exists_k110]}
    test "bitop"            {assert_equal 0  [r bitop and not_exists_k111 not_exists_k112]}

    test "pfadd"   {assert_equal 1  [r pfadd not_exists_k113 element]}
    test "pfcount" {assert_equal 0  [r pfcount not_exists_k114]}
    # test "pfdebug" {assert_equal ERR The specified key does not exist [r pfdebug 0 0 0]}
    test "pfmerge" {assert_equal OK [r pfmerge not_exists_k115 not_exists_k116]}
}