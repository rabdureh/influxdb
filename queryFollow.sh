#!/bin/sh

curl -X POST 'http://localhost:8086/db/thedb/query_follow?u=root&p=root' \
        -d '{"kw":"ixltrade","startTime":"2000-01-01 05:00:00","endTime":"2010-01-01 2:13:00}'

echo
