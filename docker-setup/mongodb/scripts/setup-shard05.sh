#!/bin/sh

CMD="$@"
/bin/sh -c "$CMD" &
sleep 10
/bin/sh -c "mongo --port 27022 < /scripts/init-shard05.js"
wait $!