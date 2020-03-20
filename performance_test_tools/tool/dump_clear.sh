source ./conf.sh

while(true)
do
    rm ${script_dir}/${data_dir}/dump/*/* -f
    sleep 20
done
