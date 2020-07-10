#!/bin/bash
uuid=$(redis-cli -p 30000 cluster nodes | grep 30000 | awk '{print $1}')

redis-cli -h 127.0.0.1  -p 30001 cluster  setslot importing $uuid {1..1000}
sleep 2s
redis-cli -h 127.0.0.1  -p 30002 cluster  setslot importing $uuid {1001..2000}  
sleep 2s
redis-cli -h 127.0.0.1  -p 30003 cluster  setslot importing $uuid {2001..3000}  
sleep 2s
redis-cli -h 127.0.0.1  -p 30004 cluster  setslot importing $uuid {3001..3276} 16381 16382 16383   
