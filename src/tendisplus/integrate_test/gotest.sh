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

./clear.sh
echo "###### repl begin ######" >> $logfile
./repl >> $logfile 2>&1

./clear.sh
echo "###### repltest begin ######" >> $logfile
./repltest >> $logfile 2>&1

./clear.sh
echo "###### restore begin ######" >> $logfile
./restore >> $logfile 2>&1

./clear.sh
echo "###### restoretest begin ######" >> $logfile
./restoretest >> $logfile 2>&1

grep "compare failed" $logfile
