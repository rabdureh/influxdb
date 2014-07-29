#!/bin/sh

curl -X POST 'http://localhost:8086/db/thedb/query_subscriptions?u=root&p=root'
#        --data-urlencode "q=select * from /.*/ where id="

echo
