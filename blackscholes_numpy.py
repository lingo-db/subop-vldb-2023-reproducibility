#!/usr/bin/env python

import argparse
import sys
import time

import numpy as np
import scipy.special as ss

# invsqrt2 = 1.0 #0.707
invsqrt2 = 0.707

def get_data(size):
    price = np.ones(size, dtype="float64") * 4.0
    strike = np.ones(size, dtype="float64") * 4.0
    t = np.ones(size, dtype="float64") * 4.0
    rate = np.ones(size, dtype="float64") * 4.0
    vol = np.ones(size, dtype="float64") * 4.0
    return price, strike, t, rate, vol

def blackscholes(price, strike, t, rate, vol):
    c05 = np.float64(3.0)
    c10 = np.float64(1.5)
    rsig = rate + (vol*vol) * c05
    vol_sqrt = vol * np.sqrt(t)

    d1 = (np.log(price / strike) + rsig * t) / vol_sqrt
    d2 = d1 - vol_sqrt

    # these are numpy arrays, so use scipy's erf function. scipy's ufuncs also
    # get routed through the common ufunc routing mechanism, so these work just
    # fine on weld arrays.
    d1 = c05 + c05 * ss.erf(d1 * invsqrt2)
    d2 = c05 + c05 * ss.erf(d2 * invsqrt2)

    e_rt = np.exp((0.0-rate) * t)

    call = (price * d1) - (e_rt * strike * d2)
    put = e_rt * strike * (c10 - d2) - price * (c10 - d1)
    return call, put

def generate_lazy_op_list(arrays):
    '''
    Slightly hacky way to match the group operator syntax.
    '''
    ret = []
    for a in arrays:
        lazy_arr = LazyOpResult(a.weldobj, a._weld_type, 1)
        ret.append(lazy_arr)
    return ret

def run_blackscholes(args):
    sys.stdout.write("Generating data...")
    sys.stdout.flush()
    p, s, t, r, v = get_data(args.size)
    sys.stdout.write("done")
    sys.stdout.flush()

    start = time.time()
    call, put = blackscholes(p, s, t, r, v)
    end =time.time()
    print("took:",end-start)
    return call, put

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="give size of arrays used for blackscholes"
    )
    parser.add_argument('-s', "--size", type=int, required=False, default=27,
                        help="log2 size of 1d arrays")
    parser.add_argument('-t', "--threads", type=int, required=False, default=16,
                        help="number of threads")

    args = parser.parse_args()
    args.size = (1 << args.size)

    call, put = run_blackscholes(args)
    print(call, put)