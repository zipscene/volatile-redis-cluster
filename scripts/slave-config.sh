MASTER_HOST=localhost
MASTER_PORT=6379
SLAVE_PORT_BEGIN=6380
SLAVE_WEIGHT="1.0"
NUM_SLAVES=`cat /proc/cpuinfo | grep '^processor[[:space:]]' | wc -l`
