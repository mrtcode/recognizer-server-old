# recognizer-server

Setup:
```
git clone https://github.com/mrtcode/recognizer-server
cd recognizer-server
./build.sh
./run_indexing.sh
docker logs -f recognizer-server
```

After indexing is completed:
```
./stop.sh
./run_normal.sh
docker logs -f recognizer-server
```