#!/bin/sh

curl -X POST 'http://localhost:8086/db/thedb/subscriptions?u=root&p=root' \
       -d '{"kws":["joe", "flacco"],"duration":1,"startTm":"2014-07-24 11:11:11","endTm":"2014-07-25 12:12:12"}'
#       -d '{"ids":[6, 5],"duration":1,"startTm":"1388534440","endTm":"1555555555"}'

echo
