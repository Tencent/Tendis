source ./conf.sh

stopshell(){
    ps axu |grep $1 |grep $user|awk '{print $2}'|xargs kill -9
}

stopshell mem.sh

stopshell qps.sh
stopshell ./stat

stopshell dump_clear.sh
