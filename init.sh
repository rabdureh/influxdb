#!/bin/bash

curl -X DELETE 'http://localhost:8086/db/mydb?u=root&p=root'
curl -X POST 'http://localhost:8086/db?u=root&p=root' \
  -d '{"name": "mydb"}'
curl -X POST -d '[{"name":"foo","columns":["val"],"points":[[23], [21], [-1], [0]]}]' 'http://localhost:8086/db/mydb/series?u=root&p=root'
