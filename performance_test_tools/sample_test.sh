rm -rf sample_test

mkdir -p sample_test/{db,log}
../build/bin/tendisplus sample_test.conf

sleep 10

BENCH_TOOL="../bin/memtier_benchmark"
TARGET_IP="127.0.0.1"
TARGET_PORT="30000"
TARGET_PASSWORD="sample_test"
TEST_TIME="300"

BENCH_CMD_PREFIX="${BENCH_TOOL} -h ${TARGET_IP} -p ${TARGET_PORT} -a ${TARGET_PASSWORD} --test-time=${TEST_TIME} --distinct-client-seed --randomize -R --data-size-range=10-100 --key-minimum=1"
BENCH_CMD_PREFIX_HIGH="${BENCH_CMD_PREFIX} -c 20 -t 5 --pipeline=10"
BENCH_CMD_PREFIX_MIDDLE="${BENCH_CMD_PREFIX} -c 10 -t 1 --pipeline=5"
BENCH_CMD_PREFIX_LOW="${BENCH_CMD_PREFIX} -c 5 -t 1 --pipeline=1"

TYPE_PREFIX="--key-prefix='kv_' --key-maximum=100000000"
# set & incrby
sleep 1 && ${BENCH_CMD_PREFIX_HIGH} --select-db=0 --command='set __key__ __data__' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_HIGH} --select-db=1 --command='incrby __key__ __int__' ${TYPE_PREFIX} &

TYPE_PREFIX="--select-db=2 --key-prefix='lish_' --key-maximum=10"
# lpush & rpush & lrange & lpop
sleep 1 && ${BENCH_CMD_PREFIX_MIDDLE} --command='lpush __key__ __data__' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_MIDDLE} --command='rpush __key__ __data__' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_LOW} --command='lrange __key__ 0 -1' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_LOW} --command='lpop __key__' ${TYPE_PREFIX} &

TYPE_PREFIX="--select-db=3 --key-prefix='hash_' --key-maximum=10"
# hset & hmset & hgetall
sleep 1 && ${BENCH_CMD_PREFIX_HIGH} --command='hset __key__ __field__ __data__' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_MIDDLE} --command='hmset __key__ __field__ __data__ __field__ __data__' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_LOW} --command='hgetall __key__' ${TYPE_PREFIX} &

TYPE_PREFIX="--select-db=4 --key-prefix='set_' --key-maximum=10"
# sadd & sdiff & sismember & smembers
sleep 1 && ${BENCH_CMD_PREFIX_HIGH} --command='sadd __key__ __field__' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_LOW} --command='sdiff __key__ __key__ __key__' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_MIDDLE} --command='sismember __key__ __field__' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_LOW} --command='smembers __key__' ${TYPE_PREFIX} &

TYPE_PREFIX="--select-db=5 --key-prefix='zset_' --key-maximum=10"
# zadd & zrange
sleep 1 && ${BENCH_CMD_PREFIX_HIGH} --command='zadd __key__ __float__ __field__' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_MIDDLE} --command='zadd __key__ __float__ __field__ __float__ __field__' ${TYPE_PREFIX} &
sleep 1 && ${BENCH_CMD_PREFIX_LOW} --command='zrange __key__ __float__ __float__' ${TYPE_PREFIX} &

# wait all done
wait $(jobs -p)
echo "Write Data Done"

# shutdown and wait for generate pgo
../bin/redis-cli -h ${TARGET_IP} -p ${TARGET_PORT} -a ${TARGET_PASSWORD} shutdown
sleep 20

rm -rf sample_test
