#!/bin/sh

curl -G 'http://localhost:8086/db/mydb/series?u=root&p=root&pretty=true' \
        --data-urlencode "q=select num_vals_id from ts_data.txt where nvkw1 =~ /location:ixl/ and num_vals_tm > 0 and num_vals_tm < 1000000000000000"

echo
