logfile="gotest.log"

rm $logfile

srcroot=`pwd`/../../../
govendor=`pwd`/../../thirdparty/govendor/
export GOPATH=$srcroot:$govendor
echo $GOPATH
go build repl.go
go build repltest.go common.go
go build restore.go common.go
go build restoretest.go common.go
go build clustertest.go common.go common_cluster.go
go build clustertestRestore.go common.go common_cluster.go

./clear.sh
echo "" >> $logfile
echo "###### repl begin ######" >> $logfile
./repl >> $logfile 2>&1

./clear.sh
echo "" >> $logfile
echo "###### repltest begin ######" >> $logfile
./repltest >> $logfile 2>&1

./clear.sh
echo "" >> $logfile
echo "###### restore begin ######" >> $logfile
./restore >> $logfile 2>&1

./clear.sh
echo "" >> $logfile
echo "###### restoretest begin ######" >> $logfile
./restoretest >> $logfile 2>&1

./clear.sh
echo "" >> $logfile
echo "###### clustertest(set) begin ######" >> $logfile
echo "### set ###" >> $logfile
./clustertest -benchtype="set" -clusterNodeNum=5 -num1=10000 >> $logfile 2>&1

#./clear.sh
#echo "### sadd ###" >> $logfile
#./clustertest -benchtype="sadd" -clusterNodeNum=5 -num1=10000 >> $logfile 2>&1

#./clear.sh
#echo "### hmset ###" >> $logfile
#./clustertest -benchtype="hmset" -clusterNodeNum=5 -num1=10000 >> $logfile 2>&1

#./clear.sh
#echo "### rpush ###" >> $logfile
#./clustertest -benchtype="rpush" -clusterNodeNum=5 -num1=10000 >> $logfile 2>&1

#./clear.sh
#echo "### zadd ###" >> $logfile
#./clustertest -benchtype="zadd" -clusterNodeNum=5 -num1=10000 >> $logfile 2>&1

./clear.sh
echo "" >> $logfile
echo "###### clustertestRestore begin ######" >> $logfile
./clustertestRestore -benchtype="set">> $logfile 2>&1

grep "go passed" $logfile
