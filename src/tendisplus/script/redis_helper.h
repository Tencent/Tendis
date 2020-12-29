// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_SCRIPT_REDIS_HELPER_H_
#define SRC_TENDISPLUS_SCRIPT_REDIS_HELPER_H_

#include <string>

void strmapchars(std::string& s, const char *from, const char *to,
    size_t setlen);


#endif  // SRC_TENDISPLUS_SCRIPT_REDIS_HELPER_H_
