#!/bin/sh

curl -G 'http://localhost:8086/db/mydb/series?u=root&p=root&pretty=true' \
        --data-urlencode "q=select * from /.*/"
echo
