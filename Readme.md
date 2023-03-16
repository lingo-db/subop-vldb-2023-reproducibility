## Required Dataset
* Fetch [New York Taxi data set (2016-01, with location data)](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?select=yellow_tripdata_2016-01.csv) and unpack it into the file `yellow_tripdata_2016-01.csv`
* Fetch [modified JOB-Dataset](https://db.in.tum.de/~schmidt/dbgen/job/imdb.tzst) and unpack the csv files into `job-data/*.csv`
## Setup
```shell
python3 -m venv venv
. venv/bin/activate
pip3 install -r requirements.txt
``` 

## Run Experiments (DuckDB+Python & Hyper)
```shell
# pagerank with scikit-network and DuckDB
python imdb-pagerank.py
# pagerank with hyper and recursive SQL
python imdb-pagerank-recursive-hyper.py
# similarity join with python and DuckDB
python imdb-similarity-join.py
# pagerank with scikit-learn and DuckDB
python taxi-kmeans.py
# k-means with hyper and recursive SQL
python imdb-kmeans-recursive-hyper.py
# blackscholes with numpy
python blackscholes-numpy.py
# haversine with numpy
python haversine-numpy.py
```

## Run Experiments (Weld)
```shell
mkdir weld-data
#generate data required for pagerank
python imdb-generate-pagerank-edges.py
line_count=$(wc -l < "weld-data/imdb-pagerank-data.csv")
line_count=$((line_count - 1))
sed -i "1s/.*/$line_count/" "weld-data/imdb-pagerank-data.csv"

# build pyweld docker container
docker build -t pyweld pyweld/
# build c++ weld docker container
docker build -t cppweld cppweld/


# run blackscholes with pyweld docker
docker run --rm -v "$(pwd)/blackscholes-weld.py:/app/blackscholes-weld.py" pyweld python /app/blackscholes-weld.py
# run haversine with pyweld docker
docker run --rm -v "$(pwd)/haversine-weld.py:/app/haversine-weld.py" pyweld python /app/haversine-weld.py

# run pagerank with cppweld docker
docker run --rm -v ./weld-data:/data:ro cppweld /experiments/build/weld_experiments pagerank
```

## Run Experiments (LingoDB)
```shell
git submodule init
git submodule update 
cd lingo-db
# install dependencies according to LingoDB Repository
[...]
git submodule update --init --recursive
make dependencies
make build-debug
make build-release
#run TPC-H and TPC-DS benchmarks
make run-benchmarks
#load job data
make resources/data/job/.stamp
cd ..
#load taxi dataset
mkdir taxidb 
./lingo-db/build/lingodb-release/sql taxidb < ./lingo-db/resources/sql/taxi/initialize.sql
cd ..
# run similarity join
./lingo-db/build/lingodb-release/run-mlir imdb-similarity-join.mlir ./lingo-db/resources/data/job
./lingo-db/build/lingodb-release/run-mlir imdb-similarity-join-pushdown.mlir ./lingo-db/resources/data/job
# run pagerank (time for iterations is reported as Timing: ... ms)
env LINGODB_TIMING=ON ./lingo-db/build/lingodb-release/run-mlir imdb-pagerank.mlir ./lingo-db/resources/data/job
# run k-means (time for iterations is reported as Timing: ... ms)
env LINGODB_TIMING=ON ./lingo-db/build/lingodb-release/run-mlir taxi-kmeans.mlir taxidb
# run blackscholes (time for actual computation is reported as Timing: ... ms)
python blackscholes-subop.py
env LINGODB_TIMING=ON ./lingo-db/build/lingodb-release/run-mlir blackscholes.mlir
# run haversine (time for actual computation is reported as Timing: ... ms)
python haversine-subop.py
env LINGODB_TIMING=ON ./lingo-db/build/lingodb-release/run-mlir haversine.mlir

```