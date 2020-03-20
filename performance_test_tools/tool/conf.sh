bin_dir=../../bin
script_dir=../
tool_dir=./
data_dir=home
ip=10.206.16.11
port=5555
benchip=172.16.0.48
benchport=5555
user=root
password=""
disk_num=2

cli_pw=""
stat_pw=""
bench_pw=""
if [ "$password" != "" ];
then
    cli_pw="-a $password"
    stat_pw="-password $password"
    bench_pw="-a $password"
fi


