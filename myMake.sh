#./cockroach quit
killall -9 cockroach
rm -rf cockroach
aws s3 rb s3://b1 --endpoint-url http://10.247.78.217:9020 --force
s3curl.pl --id=personal --createBucket -- http://10.247.78.217:9020/b1
rm -rf ./node1
#rm -rf ./node2
#rm -rf ./node3
make build
./cockroach start --background --host=localhost --store=path=./node1
#./cockroach start --background --host=localhost --store=path=./node2 --join=localhost:26257 --http-port=8082 --port=26258
#./cockroach start --background --host=localhost --store=path=./node3 --join=localhost:26257 --http-port=8083 --port=26259
