#include <string.h>

#include "release.h"
#include "version.h"
#include "tendisplus/utils/redis_port.h"

uint64_t redisBuildId(void) {
    const char* buildid = TENDISPLUS_VERSION TENDISPLUS_BUILD_ID TENDISPLUS_GIT_DIRTY TENDISPLUS_GIT_SHA1;

    return tendisplus::redis_port::crc64(0,(unsigned char*)buildid,strlen(buildid));
}
