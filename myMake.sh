#./cockroach quit
killall -9 cockroach
rm -rf cockroach
./e.sh
echo "" > log
echo "" > log2
rm -rf ./node1
#rm -rf ./node2
#rm -rf ./node3
make build
./cockroach start --background --host=localhost --store=path=./node1
#./cockroach start --background --host=localhost --store=path=./node2 --join=localhost:26257 --http-port=8082 --port=26258
#./cockroach start --background --host=localhost --store=path=./node3 --join=localhost:26257 --http-port=8083 --port=26259
