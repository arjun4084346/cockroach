#./cockroach quit
killall -9 cockroach
rm -rf cockroach
make build
<<<<<<< HEAD
./cockroach start --background --host=localhost #--port=26258 --pgport=15433
=======
./cockroach start --background --host=localhost
>>>>>>> arjun-debug
