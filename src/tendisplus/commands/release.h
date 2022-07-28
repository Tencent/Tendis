// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_COMMANDS_RELEASE_H_
#define SRC_TENDISPLUS_COMMANDS_RELEASE_H_

#define TENDISPLUS_GIT_SHA1 "d10f440f"
#define TENDISPLUS_GIT_DIRTY "269"
#define TENDISPLUS_BUILD_ID "VM_33_225_centos-1658809998"

#include <stdint.h>
uint64_t redisBuildId(void);

#endif  // SRC_TENDISPLUS_COMMANDS_RELEASE_H_
