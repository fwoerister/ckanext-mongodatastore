#!/bin/sh

CMD="$@"
/bin/sh -c "$CMD" &
sleep 30
/bin/sh -c "mongo < /scripts/init-router4shards.js"
wait $!