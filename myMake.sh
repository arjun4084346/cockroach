./cockroach quit
killall -9 cockroach
rm -rf cockroach
make build
./cockroach start --background --host=localhost #--port=26258 --pgport=15433
