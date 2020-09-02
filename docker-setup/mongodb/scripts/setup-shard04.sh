#!/bin/sh

CMD="$@"
/bin/sh -c "$CMD" &
sleep 10
/bin/sh -c "mongo --port 27021 < /scripts/init-shard04.js"
wait $!