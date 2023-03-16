import duckdb
import pandas as pd
import time
from sklearn.cluster import KMeans,DBSCAN
from tableauhyperapi import Connection, HyperException
from tableauhyperapi import CreateMode
from tableauhyperapi import HyperProcess
from tableauhyperapi import Telemetry

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
with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,parameters={"plan_cache_size": "0",
                      "experimental_index_creation": "1",
                      "experimental_storage_options": "1"
                      }) as hyper:
  with Connection(endpoint=hyper.endpoint, database="taxi.hyper", create_mode=CreateMode.CREATE_AND_REPLACE) as con:
    for statement in loadTaxi.split(";"):
      con.execute_command(statement)
    print("done loading")
    startDuckDB = time.time()
    df=con.execute_list_query("""
    with recursive trips(id,x,y,lucrativeness) as (
    select rank() over (order by p_lon, p_lat, p_t,d_t) as id, p_lon, p_lat, (fare_amount/trip_length) as lucrativeness from (select p_lon, p_lat, p_t,d_t, fare_amount,  date_part('second',d_t-p_t) as trip_length from taxi_rides  where date_part('second',d_t-p_t)>0 and date_part('hour',p_t) =20 and p_lon<50 and p_lat>30 )
    ), sample(id,x,y,lucrativeness) as (
    select  id, x,y,lucrativeness from trips limit 30
    ), clusters (iter , cid , x , y,lucrativeness) as (
    (select 0, id , x, y,lucrativeness from sample)
    union all
    select iter +1 , cid , avg(px) , avg (py),avg(lucrativeness) from (
    select iter , cid , p.x as px , p.y as py , rank () over (partition
    by p.id
    order by (p.x - c.x) *(p.x - c.x) +(p.y - c.y) *(p.y - c.y) asc , (c.x * c
    .x + c.y * c.y) asc), p.lucrativeness
    from trips p , clusters c) x
    where x.rank =1 and iter <50 group by cid , iter
    )
    select * from clusters where iter =50;
    --select cid, avg(lucrativeness) l, min(x),max(x),min(y),max(y),count(*) from clusters where iter=100 group by cid having count(*)>100 order by l desc
    """)
    endDuckDB = time.time()

    print("DuckDB:", (endDuckDB - startDuckDB))
    print(df)