#!/bin/bash

IFCONFIG=/sbin/ifconfig

if [ $# -ne 1 ]; then
	echo 'Usage: $0 <SlaveRedisIP|Interface|"guess">'
	exit 1
fi

HARG=$1
if [ "$HARG" = "guess" ]; then
	SLAVEREDISIP="`$IFCONFIG | grep 'inet addr:' | cut -d : -f 2 | awk '{print $1}' | grep -Fv 127.0.0.1 | head -n1`"
	if [ "a$SLAVEREDISIP" = "a" ]; then
		echo "Could not guess IP address."
		exit 2
	fi
else
	echo "$HARG" | grep -E '^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$' > /dev/null
	if [ $? -eq 0 ]; then
		SLAVEREDISIP="$HARG"
	else
		SLAVEREDISIP="`$IFCONFIG $HARG | grep 'inet addr:' | cut -d : -f 2 | awk '{print $1}' | head -n1`"
		if [ "a$SLAVEREDISIP" = "a" ]; then
			echo "Could not find IP for interface $HARG"
			exit 3
		fi
	fi
fi

echo "Using $SLAVEREDISIP as IP address of slave redis instances"


cd "`dirname $0`/.."

SCRIPT_DIR="`pwd`/scripts"
BASE_DIR="`pwd`"
REDIS_BASE_DIR="`pwd`/redis"

rm -rf "$REDIS_BASE_DIR"

mkdir -p "$REDIS_BASE_DIR"

. "$SCRIPT_DIR/slave-config.sh"

for((i=0;i<${NUM_SLAVES};i++)); do
	echo "Starting slave $i"
	(
		SLAVE_PORT=`expr $SLAVE_PORT_BEGIN + $i`
		SLAVE_DIR="$REDIS_BASE_DIR/slave$i"
		SLAVE_REDIS_CONF="$SLAVE_DIR/redis.conf"
		rm -rf "$SLAVE_DIR"
		mkdir -p "$SLAVE_DIR"
		cat "$SCRIPT_DIR/slave-redis.conf-template" | sed "s|__DIR__|${SLAVE_DIR}|g" | sed "s|__PORT__|${SLAVE_PORT}|g" > "$SLAVE_REDIS_CONF"
		echo "Starting redis server $i"
		redis-server "$SLAVE_REDIS_CONF"
		sleep 3
		echo "Starting slave client $i"
		cd "$BASE_DIR"
		(
			node "$BASE_DIR/lib/slave.js" -h "$SLAVEREDISIP" -p $SLAVE_PORT -w $SLAVE_WEIGHT -H "$MASTER_HOST" -P $MASTER_PORT &
			SLAVE_CLIENT_PID=$!
			echo $SLAVE_CLIENT_PID > "$SLAVE_DIR/slave-client.pid"
		) &> "$SLAVE_DIR/slave-client.log"
	) &
done

sleep 5
