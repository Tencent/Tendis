#ifndef SRC_TENDISPLUS_UTILS_PORTABLE_H_
#define SRC_TENDISPLUS_UTILS_PORTABLE_H_

#ifndef _WIN32
// NOTE(deyukong): __has_include is supported since gcc-5 series
// no need to check the existence of macro __has_include
// because we have required gcc5.5 in cmake
#if __has_include(<optional>)
#include <optional>
namespace tendisplus {
template <typename T>
using optional = std::optional<T>;
}  // namespace tendisplus
#elif __has_include(<experimental/optional>)
#include <experimental/optional>
namespace tendisplus {
template <typename T>
using optional = std::experimental::optional<T>;
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

#include <filesystem>
namespace filesystem = std::experimental::filesystem::v1;

#include <optional.h>
namespace tendisplus {
template <typename T>
using optional = std::experimental::optional<T>;
}  // namespace tendisplus
#endif


#endif  // SRC_TENDISPLUS_UTILS_PORTABLE_H_
