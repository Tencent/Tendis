export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../bin/deps

dir=home
mkdir -p ${dir}/db
mkdir -p ${dir}/dump
mkdir -p ${dir}/log

../bin/tendisplus ./tendisplus.conf
