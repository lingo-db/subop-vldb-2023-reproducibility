# import pandas as pd

import argparse
import sys
import time

import numpy as np
import pandas as pd
import os


def gen_data(size):
    sys.stdout.write("generating data...")
    sys.stdout.flush()
    lats = np.ones(size, dtype="float64") * 0.0698132
    lons = np.ones(size, dtype="float64") * 0.0698132
    sys.stdout.write("done.")
    sys.stdout.flush()
    return lats, lons


# Haversine definition
def haversine(lat2, lon2):
    miles_constant = 3959.0
    lat1 = 0.70984286
    lon1 = 1.2389197

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2.0 * np.arcsin(np.sqrt(a))
    mi = miles_constant * c
    return mi


def run_haversine_with_scalar(args):
    lat2, lon2 = gen_data(args.scale)

    print('num rows in lattitudes: ', len(lat2))
    # lat2 = weldarray(lat2)
    # lon2 = weldarray(lon2)

    start = time.time()
    dist2 = haversine(lat2, lon2)
    # dist2 = dist2.evaluate()
    end = time.time()

    print('****************************')
    print('weld took {} seconds'.format(end - start))
    print('****************************')
    print(dist2[0:5])


parser = argparse.ArgumentParser(
    description="give num_els of arrays used for nbody"
)
parser.add_argument('-s', "--scale", type=int, default=29,
                    help=("Data size"))
parser.add_argument('-t', "--threads", type=int, default=15,
                    help=("Threads"))

args = parser.parse_args()
os.environ["h"] = str(args.threads)
print("threads",args.threads)
args.scale = (1 << args.scale)
run_haversine_with_scalar(args)
