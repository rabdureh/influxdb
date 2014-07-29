#!/bin/sh

curl -G 'http://localhost:8086/db/thedb/series?u=root&p=root' \
        --data-urlencode "q=select value from /.*/"
#        &pretty=true' \
#        where time < '2015-08-13'"
#        --data-urlencode "q=select * from /.*/ where time > 1388534440 and time < 1405719776482"
echo
