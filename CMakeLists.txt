cmake_minimum_required (VERSION 3.13.0)
project (TENDIS_PLUS)

execute_process(COMMAND ./mkreleasehdr.sh WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

option(TENDIS_DEBUG "define TENDIS_DEBUG" OFF)
if(TENDIS_DEBUG)
    add_definitions(-DTENDIS_DEBUG)
endif()

option(COM_OPT_LTO "compile optimize with lto" OFF)
option(COM_OPT_PGO_GEN "compile optimize with pgo, profile-generate" OFF)
option(COM_OPT_PGO_USE "compile optimize with pgo, profile-use" OFF)

if(COM_OPT_LTO)
    message("compile optimize with lto")
    set(CMAKE_AR "gcc-ar")
    set(CMAKE_NM "gcc-nm")
    set(CMAKE_RANLIB "gcc-ranlib")

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto=auto")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -flto=auto")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto=auto")
endif()

set(pgo_dir ${PROJECT_SOURCE_DIR}/pgo)
message("pgo_dir: ${pgo_dir}")
if(COM_OPT_PGO_GEN)
    message("compile optimize with pgo, profile-generate")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -fprofile-dir=${pgo_dir} -fprofile-generate=${pgo_dir}")
endif()
if(COM_OPT_PGO_USE)
    message("compile optimize with pgo, profile-use")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -fprofile-dir=${pgo_dir} -fprofile-use=${pgo_dir} -fprofile-correction -fprofile-partial-training -Wno-error=missing-profile")
endif()

# configure compiler requirement
message(${CMAKE_CXX_COMPILER})
if(CMAKE_COMPILER_IS_GNUCC)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8)
        message(FATAL_ERROR "g++ version ${CMAKE_CXX_COMPILER_VERSION}, Require at least gcc-8, \
        you may remove CMakeCache.txt and run export CXX=/path/to/your/prefered/cxx_compiler before run cmake \
        to solve this problem")
    endif()
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g3 -ggdb3 -gdwarf-4 -fvar-tracking-assignments -fno-omit-frame-pointer")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g3 -ggdb3 -gdwarf-4 -fvar-tracking-assignments -fno-omit-frame-pointer")
elseif(MSVC AND MSVC_VERSION GREATER_EQUAL 1900)
	message("using MSVC ${MSVC_VERSION}")
	# TODO: check msvc version
	# for rocksdb, disable warning as errors
	add_compile_options(/WX-)

	# for glog
	add_definitions(-DGLOG_NO_ABBREVIATED_SEVERITIES)

	# for asio
	#add_definitions(-DWIN32_LEAN_AND_MEAN)

	# disable warnings
	#add_compile_options(/wC4099)
else()
    message(FATAL_ERROR "gcc or vs2015(update 2)+ is required")
endif()

set(CMAKE_C_STANDARD 17)
set(CMAKE_CXX_STANDARD 17)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
else()
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
endif()

message("using rocksdb-v8.5.3")
if(CMAKE_COMPILER_IS_GNUCC)
    set(patchCommand
        "cd ${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb/rocksdb\;
         git am --abort\;
         git am -s ../../patch/0001-add-statistic-for-sst-file-info.patch\;
         git am -s ../../patch/0002-add-latency-statistic-log.patch\;
         git am -s ../../patch/0003-better-control-of-blobldb-space-amp.patch\;
         git am -s ../../patch/0004-reclaim-blob-files-after-deleting-all-ssts.patch\;
         ")
    execute_process(COMMAND bash "-c" ${patchCommand})
endif()
set(ROCKSDB_DIR "${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb")
set(CMAKE_MODULE_PATH "${ROCKSDB_DIR}/cmake/modules/")
add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DROCKSDB_SUPPORT_THREAD_LOCAL)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

option(WITH_ASAN "build with ASAN" OFF)
option(WITH_TSAN "build with WITH_TSAN" OFF)
option(ENABLE_JEMALLOC "enable jemalloc" ON)
if(WITH_ASAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fno-common -static-libasan")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-common -static-libasan")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fno-common -static-libasan")
  set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fsanitize=address -fno-common -static-libasan")
  list(APPEND SYS_LIBS "-fsanitize=address -fno-common -static-libasan")
  add_definitions(-DWITH_ASAN)
  set(WITH_TSAN OFF)
  message("ASAN won't work with TSAN")
  set(ENABLE_JEMALLOC OFF)
  message("ASAN would disabled JeMalloc")
endif()

if(WITH_TSAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread -pie -lpthread -lm -static-libtsan")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fPIC -lpthread -lm -static-libtsan")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread -fPIC -lpthread -lm -static-libtsan")
  set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fsanitize=thread -fPIC -lpthread -lm -static-libtsan")
  list(APPEND SYS_LIBS "-fsanitize=thread -lpthread -lm -static-libtsan")
  add_definitions(-DWITH_TSAN)
  set(WITH_ASAN OFF)
  message("TSAN won't work with ASAN")
  set(ENABLE_JEMALLOC OFF)
  message("TSAN would disabled JeMalloc")
endif()

option(WITH_COVER "build with COVER" OFF)
if(WITH_COVER)
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
   set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
   add_definitions(-DWITH_COVER)
endif()

if(CMAKE_COMPILER_IS_GNUCC)
	if(ENABLE_JEMALLOC)
		set(JEMALLOC_ROOT_DIR "${PROJECT_SOURCE_DIR}/src/thirdparty/jemalloc")
		add_definitions(-DTENDIS_JEMALLOC)

		set(patchCommand "cd ${PROJECT_SOURCE_DIR}/src/thirdparty/jemalloc\; git apply ../patch/jemalloc_patch.diff")
		message("jemalloc git patch, we force set jemalloc  prof:true")
		execute_process(COMMAND bash "-c" ${patchCommand} OUTPUT_VARIABLE FOO)

		set(WITH_JEMALLOC ON)

		add_custom_command(OUTPUT "${JEMALLOC_ROOT_DIR}/include/jemalloc/jemalloc.h"
                # NOTE(takenliu) support jemalloc profile, before start tendisplus need export as below:
                #     export MALLOC_CONF="prof:true,prof_prefix:jeprof.out"
				COMMAND ./autogen.sh --enable-prof --disable-shared "CFLAGS=-Wno-missing-attributes -g3 -ggdb3 -gdwarf-4" "CXXFLAGS=-Wno-missing-attributes -g3 -ggdb3 -gdwarf-4"
				WORKING_DIRECTORY ${JEMALLOC_ROOT_DIR})
		add_custom_command(OUTPUT "${JEMALLOC_ROOT_DIR}/lib/libjemalloc_pic.a"
				COMMAND make -j16
				DEPENDS "${JEMALLOC_ROOT_DIR}/include/jemalloc/jemalloc.h"
				WORKING_DIRECTORY ${JEMALLOC_ROOT_DIR}
				COMMENT "Making external library jemalloc")
		add_custom_target(build_jemalloc
				DEPENDS "${JEMALLOC_ROOT_DIR}/lib/libjemalloc_pic.a")

		add_library(jemalloc STATIC IMPORTED)
		set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION "${JEMALLOC_ROOT_DIR}/lib/libjemalloc_pic.a")

		list(APPEND SYS_LIBS jemalloc)
		list(APPEND THIRDPARTY_LIBS jemalloc)
		add_dependencies(jemalloc build_jemalloc)

		set(JeMalloc_INCLUDE_DIRS ${JEMALLOC_ROOT_DIR}/include)
		link_directories(${JEMALLOC_ROOT_DIR}/lib/)
		set(JeMalloc_LIBRARIES ${JEMALLOC_ROOT_DIR}/lib/libjemalloc_pic.a)
	endif()

	list(APPEND SYS_LIBS "dl" "pthread" "rt")
endif()

# TODO(vinchen): in the future it should disable sync_ponit in production
add_definitions(-DWITH_SYNC_POINT)
add_subdirectory(src/thirdparty/gflag)
set(SNAPPY_BUILD_TESTS OFF)
add_subdirectory(src/thirdparty/snappy)
target_compile_options(snappy PRIVATE -fPIC)
add_subdirectory(src/thirdparty/lz4/contrib/cmake_unofficial EXCLUDE_FROM_ALL)
add_subdirectory(src/thirdparty/googletest)
set(WITH_GFLAGS OFF)
add_subdirectory(src/thirdparty/glog EXCLUDE_FROM_ALL)

list(APPEND SYS_LIBS sync_point)
list(APPEND SYS_LIBS snappy)

set(WITH_TESTS OFF)
set(WITH_BENCHMARK_TOOLS OFF)
set(WITH_TOOLS OFF)
set(WITH_JNI OFF)
set(WITH_GFLAGS OFF)
set(WITH_CORE_TOOLS OFF)
set(WITH_TOOLS OFF)

add_definitions(-DSNAPPY)
set(snappy_FOUND TRUE)
set(snappy_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/src/thirdparty/include/snappy/)
set(snappy_LIBRARIES ${CMAKE_BINARY_DIR}/lib/libsnappy.a)
list(APPEND THIRDPARTY_LIBS snappy)
set(LZ4_FOUND TRUE)
set(LZ4_LIBRARIES ${CMAKE_BINARY_DIR}/lib/liblz4.a)
include_directories("src/thirdparty/lz4/lib/")
list(APPEND THIRDPARTY_LIBS ${LZ4_LIBRARIES})

option(RAPIDJSON_BUILD_TESTS "" OFF)
set(RAPIDJSON_BUILD_DOC OFF)
set(RAPIDJSON_BUILD_EXAMPLES OFF)
set(RAPIDJSON_BUILD_TESTS OFF)
set(RAPIDJSON_HAS_STDSTRING ON)
add_subdirectory(src/thirdparty/rapidjson)
add_subdirectory(src/thirdparty/lua)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-maybe-uninitialized") 
add_subdirectory(${ROCKSDB_DIR}/rocksdb EXCLUDE_FROM_ALL)
target_compile_options(rocksdb PRIVATE -DLZ4)

if(CMAKE_COMPILER_IS_GNUCC)
set(WARN_C_CXX
"-Wall \
-Wextra \
-Wno-unused-but-set-variable \
-Wno-unused-parameter \
-Wno-unused-local-typedefs \
-Wno-missing-field-initializers \
-Wno-implicit-fallthrough \
-Wendif-labels \
-Wno-float-equal \
-Wformat=2 \
-Wframe-larger-than=69632 \
-Wmissing-include-dirs \
-Wpointer-arith \
-Wwrite-strings \
-Werror=char-subscripts \
-Werror=comments \
-Werror=conversion-null \
-Werror=empty-body \
-Werror=endif-labels \
-Werror=format \
-Werror=format-nonliteral \
-Werror=missing-include-dirs \
-Werror=overflow \
-Werror=parentheses \
-Werror=reorder \
-Werror=return-type \
-Werror=sequence-point \
-Werror=sign-compare \
-Werror=switch \
-Werror=type-limits \
-Werror=uninitialized \
-Wno-maybe-uninitialized \
-Werror=unused-function \
-Werror=unused-label \
-Werror=unused-result \
-Werror=unused-value \
-Werror=unused-variable \
-Werror=write-strings")

set(WARN_CXX_ONLY
"-Wno-invalid-offsetof \
-Wnon-virtual-dtor \
-Woverloaded-virtual \
-Wvla \
-Werror=non-virtual-dtor \
-Werror=overloaded-virtual \
-Werror=vla")

set(WARN_C_ONLY
"-Werror-implicit-function-declaration")

# gflags

include_directories("src/thirdparty/include")
include_directories("src/thirdparty/include/snappy/")
include_directories("src/thirdparty/lz4/lib/")
else()
include_directories("${PROJECT_SOURCE_DIR}/src/tendisplus/include")
#include_directories("${PROJECT_SOURCE_DIR}/src/tendisplus/include/snappy")
#include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/lz4/lib/")

# gflags
include_directories("src/thirdparty/include/Windows/")
ENDIF()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${WARN_C_CXX} ${WARN_CXX_ONLY}")

add_definitions(-DASIO_STANDALONE)
add_definitions(-DRAPIDJSON_HAS_STDSTRING)

message("CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message("CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message("CMAKE_CXX_FLAGS_RELEASE: ${CMAKE_CXX_FLAGS_RELEASE}")
include_directories("${PROJECT_SOURCE_DIR}/src")

include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/googletest/googletest/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/asio/asio/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/glog")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/glog/src")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/jemalloc/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/rapidjson/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/lua/src")
include_directories(${ROCKSDB_DIR})
include_directories(${ROCKSDB_DIR}/rocksdb)
include_directories(${ROCKSDB_DIR}/rocksdb/include)

option(TENDIS_WITH_TOOLS "build ldb_tendis and other tools" ON)
option(TENDIS_WITH_UNIT_TESTS "build unit tests or not" ON)
add_subdirectory(src/tendisplus/network)
add_subdirectory(src/tendisplus/utils)
add_subdirectory(src/tendisplus/server)
add_subdirectory(src/tendisplus/commands)
add_subdirectory(src/tendisplus/storage)
add_subdirectory(src/tendisplus/replication)
add_subdirectory(src/tendisplus/cluster)
add_subdirectory(src/tendisplus/lock)
add_subdirectory(src/tendisplus/script)
if(NOT WITH_ASAN AND NOT WITH_TSAN AND TENDIS_WITH_TOOLS)
add_subdirectory(src/tendisplus/tools)
endif()
