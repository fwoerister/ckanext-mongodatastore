#!/bin/bash
set -e

echo "This is travis-build.bash..."

cd docker-setup
docker-compose up --build -d

exit 0
