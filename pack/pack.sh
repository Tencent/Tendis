version=1.0
gcc_version=5.5.0
root_dir=../

packname=tendisplus-${version}
rm ${packname}_back -rf
mv $packname ${packname}_back
mkdir -p $packname
mkdir -p $packname/bin
mkdir -p $packname/bin/deps
mkdir -p $packname/scripts

cp ${root_dir}/build/bin/tendisplus $packname/bin
cp ${root_dir}/build/bin/binlog_tool $packname/bin
cp ${root_dir}/bin/redis-cli $packname/bin
cp /usr/local/gcc-${gcc_version}/lib64/libstdc++.so.6 $packname/bin/deps
cp ${root_dir}/pack/start.sh $packname/scripts
cp ${root_dir}/pack/stop.sh $packname/scripts
cp ${root_dir}/pack/tendisplus.conf $packname/scripts

mv ${packname}.tgz ${packname}_back.tgz
tar -cvzf ${packname}.tgz ${packname}/*

echo -e "\033[32mpack success: ${packname}.tgz \033[0m"
