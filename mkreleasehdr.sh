#!/bin/bash

GIT_SHA1=`(git show-ref --head --hash=8 2> /dev/null || echo 00000000) | head -n1`
GIT_DIRTY=`git diff --no-ext-diff 2> /dev/null | wc -l`
BUILD_ID=`uname -n`"-"`date +%s`
if [ -n "$SOURCE_DATE_EPOCH" ]; then
  BUILD_ID=$(date -u -d "@$SOURCE_DATE_EPOCH" +%s 2>/dev/null || date -u -r "$SOURCE_DATE_EPOCH" +%s 2>/dev/null || date -u %s)
fi

subdir="src/tendisplus/commands/"
h_file=${subdir}/release.h
c_file=${subdir}/release.cpp

test -f ${h_file} || touch ${h_file}
(cat ${h_file} | grep SHA1 | grep $GIT_SHA1) && \
(cat ${h_file} | grep DIRTY | grep $GIT_DIRTY) && exit 0 # Already up-to-date

echo "// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved." > ${h_file}
echo "// Please refer to the license text that comes with this tendis open source" >> ${h_file}
echo "// project for additional information." >> ${h_file}
echo "" >> ${h_file}

echo "#ifndef SRC_TENDISPLUS_COMMANDS_RELEASE_H_" >> ${h_file}
echo "#define SRC_TENDISPLUS_COMMANDS_RELEASE_H_" >> ${h_file}

echo "" >> ${h_file}
echo "#define TENDISPLUS_GIT_SHA1 \"$GIT_SHA1\"" >> ${h_file}
echo "#define TENDISPLUS_GIT_DIRTY \"$GIT_DIRTY\"" >> ${h_file}
echo "#define TENDISPLUS_BUILD_ID \"$BUILD_ID\"" >> ${h_file}

echo "" >> ${h_file}
echo "#include <stdint.h>" >> ${h_file}
echo "uint64_t redisBuildId(void);" >> ${h_file}

echo "" >> ${h_file}
echo "#endif  // SRC_TENDISPLUS_COMMANDS_RELEASE_H_" >> ${h_file}


touch ${c_file} # Force recompile of release.c

