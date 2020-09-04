#!/bin/sh

CMD="$@"
/bin/sh -c "$CMD" &
sleep 10
/bin/sh -c "mongo --port 27023 < /scripts/init-shard06.js"
wait $!