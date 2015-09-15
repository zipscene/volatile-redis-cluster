#!/bin/bash
cd "`dirname $0`/.."
KILLARGS=""
while read -r pidfile; do
	PID=`cat $pidfile`
	KILLARGS="${KILLARGS} ${PID}"
done <<< "`find ./redis -iname '*.pid'`"
kill $KILLARGS

