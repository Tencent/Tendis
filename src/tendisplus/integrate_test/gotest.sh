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
echo "###### clustertest begin ######" >> $logfile
./clustertest -benchtype="set" -clusterNodeNum=5 -num1=10000 >> $logfile 2>&1

./clear.sh
echo "" >> $logfile
echo "###### clustertestRestore begin ######" >> $logfile
./clustertestRestore -benchtype="set">> $logfile 2>&1

grep "go passed" $logfile
