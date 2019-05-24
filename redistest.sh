tclsh tests/test_helper.tcl --single rr_unit/type/string
tclsh tests/test_helper.tcl  --single  rr_unit/type/hash
#tclsh tests/test_helper.tcl  --single  rr_unit/type/hscan
tclsh tests/test_helper.tcl  --single  rr_unit/type/list-2
tclsh tests/test_helper.tcl  --single  rr_unit/type/list-3
#tclsh tests/test_helper.tcl  --single  rr_unit/type/list-common
tclsh tests/test_helper.tcl  --single  rr_unit/type/list
tclsh tests/test_helper.tcl  --single  rr_unit/type/set
tclsh tests/test_helper.tcl  --single  rr_unit/type/zset
tclsh tests/test_helper.tcl  --single  rr_unit/hyperloglog
tclsh tests/test_helper.tcl  --single rr_unit/expire
tclsh tests/test_helper.tcl --single rr_unit/bitops
tclsh tests/test_helper.tcl --single rr_unit/auth
tclsh tests/test_helper.tcl --single rr_unit/basic
tclsh tests/test_helper.tcl --single rr_unit/protocol  
tclsh tests/test_helper.tcl --single rr_unit/other

tclsh tests/test_helper.tcl --single rr_unit/quit
tclsh tests/test_helper.tcl --single rr_unit/sort
tclsh tests/test_helper.tcl --single rr_unit/bugs

