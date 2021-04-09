// Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
// Please refer to the license text that comes with this tendis open source
// project for additional information.

#ifndef SRC_TENDISPLUS_UTILS_PORTABLE_H_
#define SRC_TENDISPLUS_UTILS_PORTABLE_H_

#ifndef _WIN32
// NOTE(deyukong): __has_include is supported since gcc-5 series
// no need to check the existence of macro __has_include
// because we have required gcc5.5 in cmake
#if __has_include(<experimental/optional>)
#include <experimental/optional>
namespace tendisplus {
template <typename T>
using optional = std::experimental::optional<T>;
}  // namespace tendisplus
#elif __has_include(<optional>)
#include <optional>
namespace tendisplus {
template <typename T>
using optional = std::optional<T>;
}  // namespace tendisplus
#else
#error "no available optional headfile"
#endif  // __has_include(<optional>)

#if __has_include(<filesystem>)
#include <filesystem>
namespace tendisplus {
namespace filesystem = std::filesystem;
}  // namespace tendisplus
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace tendisplus {
namespace filesystem = std::experimental::filesystem;
}  // namespace tendisplus
#else
#error "no available filesystem headfile"
#endif  // __has_include(<filesystem>)
#else

#if _MSC_VER > 1900
#define _SILENCE_EXPERIMENTAL_FILESYSTEM_DEPRECATION_WARNING
#include <experimental/filesystem>  // C++-standard header file name
#endif
#include <filesystem>  // Microsoft-specific implementation header file name
namespace tendisplus {
namespace filesystem = std::experimental::filesystem::v1;
}  // namespace tendisplus

#include <optional.h>  // NOLINT
namespace tendisplus {
template <typename T>
using optional = std::experimental::optional<T>;
}  // namespace tendisplus
#endif


#endif  // SRC_TENDISPLUS_UTILS_PORTABLE_H_
