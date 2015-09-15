#!/bin/bash
PIDFILE="`dirname $0`/../coordinator.pid"
kill `cat $PIDFILE`

