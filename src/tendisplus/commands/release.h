// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_COMMANDS_RELEASE_H_
#define SRC_TENDISPLUS_COMMANDS_RELEASE_H_

#define TENDISPLUS_GIT_SHA1 "b7722570"
#define TENDISPLUS_GIT_DIRTY "127"
#define TENDISPLUS_BUILD_ID "VM-98-57-centos-1603249639"

#include <stdint.h>
uint64_t redisBuildId(void);

#endif  // SRC_TENDISPLUS_COMMANDS_RELEASE_H_
