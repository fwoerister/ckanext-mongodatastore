#!/bin/sh

CMD="$@"
/bin/sh -c "$CMD" &
sleep 10
/bin/sh -c "mongo --port 27019 < /scripts/init-shard02.js"
wait $!