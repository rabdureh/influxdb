#/bin/sh

curl -X POST 'http://localhost:8086/db/mydb/users?u=root&p=root' \
        -d '{"name": "thumps", "password": "chupee"}'
