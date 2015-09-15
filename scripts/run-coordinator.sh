#!/bin/bash
cd "`dirname $0`/.."
(
	node ./lib/coordinator.js &
	echo $! > ./coordinator.pid
) > ./coordinator.log
echo "Coordinator started."

