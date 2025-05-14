files=`git diff --name-only HEAD HEAD~1`
echo "$files" | while read file
do
    if [[ $file =~ \.cc$ || $file =~ \.h$ || $file =~ \.hpp$ || $file =~ \.cpp$ ]]; then
        if [[ -f $file ]]; then
            if [[ $file =~ optional\.h$ ]]; then
                continue
            else
                echo "cpplint --filter=\"-legal/copyright,-runtime/reference,-build/c++11,-build/include_subdir\" $file"
                #cpplint --filter="-legal/copyright,-runtime/reference,-build/header_guard,-build/c++11,-build/include_subdir" $file
               /usr/local/bin/cpplint --filter="-build/c++11,-runtime/reference,-whitespace/indent_namespace" $file
                if [[ $? -ne 0 ]]; then
                    echo "Run cpplint against $file failed..."
                    exit 1
                fi
                #clang-format --style=file $file > formatTMP
                #~/bin/clang-format --style=file $file > formatTMP
                #/data/git/llvm/llvm-project/build/bin/clang-format --style=file $file > formatTMP
                /bin/clang-format --style=file $file > formatTMP
                a=$(diff $file formatTMP | wc -l)
                if [[ "$a" -ne "0" ]]; then
                    #echo "Run clang-format against $file failed..."
                    #diff formatTMP $file
                    echo "=========clang-format fix the file: $file======="
                    cp formatTMP $file
                fi
            fi
        fi
    fi
    if [[ $file =~ \.go$ ]]; then
        gofmt -d $file
    fi
done
