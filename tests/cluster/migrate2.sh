#!/bin/bash
uuid1=$(redis-cli -p 30001 cluster nodes | grep 30000 | awk '{print $1}')
uuid2=$(redis-cli -p 30002 cluster nodes | grep 30000 | awk '{print $1}')
uuid3=$(redis-cli -p 30003 cluster nodes | grep 30000 | awk '{print $1}')
uuid4=$(redis-cli -p 30004 cluster nodes | grep 30000 | awk '{print $1}')

redis-cli -h 127.0.0.1  -p 30000 cluster  setslot importing $uuid1 {1..1000}
redis-cli -h 127.0.0.1  -p 30000 cluster  setslot importing $uuid2 {1001..2000}  
redis-cli -h 127.0.0.1  -p 30000 cluster  setslot importing $uuid3 {2001..3000}  
redis-cli -h 127.0.0.1  -p 30000 cluster  setslot importing $uuid4 {3001..3276} 16381 16382 16383  


