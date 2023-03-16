import duckdb
import pandas as pd
import time
from sklearn.cluster import KMeans,DBSCAN

import os
loadTaxi ="""
create table taxi_rides(
    vendor_id	integer NOT NULL,
    p_t	timestamp NOT NULL,
    d_t	timestamp NOT NULL,
    passenger_count	integer NOT NULL,
    trip_distance	float NOT NULL,
    p_lon	float8 NOT NULL,
    p_lat	float8 NOT NULL,
    ratecode_id	integer NOT NULL,
    store_and_fwd_flag	text NOT NULL,
    d_lon	float8 NOT NULL,
    d_lat	float8 NOT NULL,
    payment_type	integer NOT NULL,
    fare_amount	decimal(9,2) NOT NULL,
    extra	decimal(9,2) NOT NULL,
    mta_tax	decimal(9,2) NOT NULL,
    tip_amount	decimal(9,2) NOT NULL,
    tolls_amount	decimal(9,2) NOT NULL,
    improvement_surcharge	decimal(9,2) NOT NULL,
    total_amount	decimal(9,2) NOT NULL
);
copy taxi_rides from 'yellow_tripdata_2016-01.csv' csv;
"""
con = duckdb.connect(database=':memory:')
con.execute(loadTaxi)
con.execute("SET threads TO 1;");

startDuckDB = time.time()
df=con.execute("select p_lon, p_lat, (fare_amount/trip_length) as lucrativeness from (select p_lon, p_lat, fare_amount,  date_diff('second',p_t,d_t) as trip_length from taxi_rides  where trip_length>0 and date_part('hour',p_t) =20 and p_lon<50 and p_lat>30  )").fetchdf()
df.to_csv('weld-data/taxi-kmeans-data.csv')