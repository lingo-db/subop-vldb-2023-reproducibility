## Required Dataset
* Fetch [New York Taxi data set (2016-01, with location data)](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?select=yellow_tripdata_2016-01.csv) and unpack it into the file `yellow_tripdata_2016-01.csv`
* Fetch [modified JOB-Dataset](https://db.in.tum.de/~schmidt/dbgen/job/imdb.tzst) and unpack the csv files into `job-data/*.csv`
## Setup
```shell
python3 -m venv venv
. venv/bin/activate
pip3 install -r requirements.txt
``` 

## Run Experiments (DuckDB+ Python)
```shell
env OMP_NUM_THREADS=1 python imdb-pagerank.py
env OMP_NUM_THREADS=1 python imdb-similarity-join.py
env OMP_NUM_THREADS=1 python taxi-kmeans.py
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
# run examples
./lingo-db/build/lingodb-release/run-mlir imdb-pagerank.mlir ./lingo-db/resources/data/job
./lingo-db/build/lingodb-release/run-mlir imdb-similarity-join.mlir ./lingo-db/resources/data/job
./lingo-db/build/lingodb-release/run-mlir taxi-kmeans.mlir taxidb
```