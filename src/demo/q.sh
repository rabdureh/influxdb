#!/bin/sh

curl -G 'http://localhost:8086/db/mydb/series?u=root&p=root&pretty=true' \
        --data-urlencode "q=select * from ts_data.txt where num_vals_tm > 946684800 and num_vals_tm < 1404993600"
echo
