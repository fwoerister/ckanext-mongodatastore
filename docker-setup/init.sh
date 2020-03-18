#!/bin/bash

docker-compose exec mdb-config sh -c "mongo --port 27017 < /scripts/init-configserver.js"
docker-compose exec mdb-shard01 sh -c "mongo --port 27018 < /scripts/init-shard01.js"
docker-compose exec mdb-shard02 sh -c "mongo --port 27019 < /scripts/init-shard02.js"
docker-compose exec mdb-shard03 sh -c "mongo --port 27020 < /scripts/init-shard03.js"
sleep 20
docker-compose exec mongodb sh -c "mongo < /scripts/init-router.js"
