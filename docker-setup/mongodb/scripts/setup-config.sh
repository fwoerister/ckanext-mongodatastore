#!/bin/sh

CMD="$@"
/bin/sh -c "$CMD" &
sleep 10
/bin/sh -c "mongo --port 27017 < /scripts/init-configserver.js"
wait $!