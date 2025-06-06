if [[ $# -eq 1 && $1 == "--usage" ]]
then
    echo "./cpplint.sh [lastCommitNum]"
    exit
fi

lastCommitNum=1
if [[ $# -eq 1 ]]
then
    if [[ ! $2 =~ ^[0-9]+$ ]]; then
        echo "error, lastCommitNum must be a number."
    exit
    fi
    lastCommitNum=$1
fi

files=`git diff --name-only HEAD HEAD~$lastCommitNum`
echo "$files" | while read file
do
    if [[ $file =~ \.cc$ || $file =~ \.h$ || $file =~ \.hpp$ || $file =~ \.cpp$ ]]; then
        if [[ -f $file ]]; then
            if [[ $file =~ optional\.h$ ]]; then
                continue
            else
                clang-format --style=file $file > formatTMP
                a=$(diff $file formatTMP | wc -l)
                if [[ "$a" -ne "0" ]]; then
                    echo "=========clang-format fix the file: $file======="
                    cp formatTMP $file
                else
                    echo "clang-format ok $file"
                fi
                cpplint --filter="-build/c++11,-runtime/reference,-whitespace/indent_namespace" $file
                if [[ $? -ne 0 ]]; then
                    echo "Run cpplint against $file failed..."
                    exit 1
                fi
            fi
        fi
    fi
    if [[ $file =~ \.go$ ]]; then
        gofmt -d $file
    fi
done
