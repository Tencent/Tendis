#!/bin/sh

dir=build/bin

./$dir/record_test
#./logging_unittest
#./utilities_unittest
#./db_sanity_test
./$dir/lock_test
./$dir/mgl_test
./$dir/signalhandler_unittest
./$dir/symbolize_unittest
./$dir/atomic_utility_test
./$dir/index_mgr_test
./$dir/stacktrace_unittest
./$dir/status_test
./$dir/skiplist_test
./$dir/varint_test
./$dir/server_params_test
./$dir/commmand_test
./$dir/demangle_unittest
./$dir/stl_logging_unittest
./$dir/network_test
./$dir/rocks_kvstore_test
