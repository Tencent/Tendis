user=$USER
baseCmd="ps aux | grep ${user} | grep -v grep | grep -v gotest.sh"
if [[ "$1" == "versiontest" ]]; then
    processNameList=(tendisplus predixy)
    baseCmd="${baseCmd} | grep _version_test_"
else
    rm -rf m*_* s*_* running slowlog benchmark_*.log predixy_* dst_* src_* redis-sync* sync* dts.log jeprof*
    cd dts; rm -rf ./m*_* ./s*_* ./t*_* ./running; cd ..

    processNameList=(tendisplus predixy redis-server redis-sync checkdts)
    baseCmd="${baseCmd} | grep -v _version_test_"
fi

for pName in ${processNameList[@]}; do
    function getpids() {
        # only tendisplus and predixy contains 'integrate_test' in process command
        localCmd="${baseCmd}"
        if [[ "${pName}" == "tendisplus" || "${pName}" == "predixy" ]]; then
            localCmd="${localCmd} | grep integrate_test"
        fi
        localCmd="${localCmd} | grep ${pName} | awk "\''{print $2}'\'
        echo ${localCmd}
        pids=$(bash -c "${localCmd}")
    }
    getpids
    for pid in $pids; do
        echo "killing ${pName}: ${pid}"
        kill -9 ${pid}
    done
    while true; do
        getpids
        # check if pid is empty
        if [[ -z "${pids// }" ]]; then
            break
        fi
        sleep 1
    done
done
