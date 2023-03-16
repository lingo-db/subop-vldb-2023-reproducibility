#include <string>
#include <vector>
#include <fstream>
#include <sys/time.h>
#include <iostream>
#include "../experiments.h"
#include "weld.h"

namespace {
    template<typename T>
    struct weld_vector {
        T *data;
        int64_t length;
    };

    struct k_means_args {
        weld_vector<weld_vector<double>> data_vec;
        weld_vector<weld_vector<double>> means;
        int64_t k;
        int64_t iters;
    };

    template<typename T>
    weld_vector<T> make_weld_vector(T *data, int64_t length) {
        struct weld_vector<T> vector;
        vector.data = data;
        vector.length = length;
        return vector;
    }

    std::string kmeans_code = R"WELD(
macro norm(x, y) = (
    result(for(zip(x, y), merger[f64, +], |b,i,e| merge(b, (e.$0-e.$1)*(e.$0-e.$1))))
);

# Get the index of the minimum element.
macro argmin(x) = (
    let min_index = 0L;
    let cur_index = 0L;
    let res = iterate({min_index, cur_index},
	|e| if (lookup(x, e.$1) < lookup(x, e.$0),
	    if (e.$1 < (len(x)-1L), {{e.$1, e.$1+1L}, true}, {{e.$1, e.$1+1L}, false}),
	    if (e.$1 < (len(x)-1L), {{e.$0, e.$1+1L}, true}, {{e.$0, e.$1+1L}, false})
	)
    );
    res.$0
);

# Get the index of the closest mean.
macro argmin_norm(point, means) = (
    let min_index = 0L;
    let cur_index = 0L;
    let min_norm = norm(point, lookup(means, 0L));
    let res = iterate({{min_index, min_norm}, cur_index},
	|e|
	let cur_norm = norm(point, lookup(means, e.$1));
	if (cur_norm < e.$0.$1,
	if (e.$1 < (len(means)-1L), {{{e.$1, cur_norm}, e.$1+1L}, true}, {{{e.$1, cur_norm}, e.$1+1L}, false}),
	if (e.$1 < (len(means)-1L), {{e.$0, e.$1+1L}, true}, {{e.$0, e.$1+1L}, false})
	)
    );
    res.$0.$0
);


# Main k-means implementation.
# Choice of initial means is delegated to the user for now.
|x:vec[vec[f64]], means: vec[vec[f64]], k: i64, iters: i64| # each point is a d-dimensional vector
# do some preprocessing
# this operation should be distributed on the cluster
#let inputs = result(for(x,
#	appender[vec[f64]],
#	|b,i,e| merge(b,
#	    result(for(e,
#		    appender[f64],
#		    |b2,i2,e2| merge(b2, e2*2.0))))));
let inputs = x;

let means_init = result(for(means,
	appender[vec[f64]],
	|b,i,e| merge(b,
	    result(for(e,
		    appender[f64],
		    |b2,i2,e2| merge(b2, e2))))));

let iter_idx = 0L;
let res = iterate({means_init, iter_idx},
    |z|
    let time = cudf[print_time,i32]();

    # Find the index of the min mean for each point.
    # distributed
    let cluster_assignments =
    result(for(inputs,
	    appender[i64](len(inputs)),
	    |b,i,point| merge(b, argmin_norm(point, z.$0))));

    # create a flattened list of means (on driver)
    let d = len(lookup(inputs, 0L));
    let sums_init = result(for(rangeiter(0L, k*d, 1L),
	    appender[f64], # concatenation of k length-d sums
	    |b,i,e| merge(b, 0.0)
	));

    let counts_init = result(for(rangeiter(0L, k, 1L),
	    appender[i64],
	    |b,i,e| merge(b, 0L)
	));

    # Individual sums and counts will be distributed, then final vecmerger happens on driver
    let elemwise_aggregate = for(zip(inputs, cluster_assignments),
	vecmerger[f64, +](sums_init),
	|b,i,e|
	for(e.$0, b,
	    |b2, i2, e2| merge(b, {e.$1*d + i2, e2})
	)
    );

    let counts_aggregate = for(cluster_assignments,
	vecmerger[i64, +](counts_init),
	|b,i,e| merge(b, {e, 1L}));

    let elemwise_sums = result(elemwise_aggregate);
    let cluster_counts = result(counts_aggregate);

    # reshape into one d-dimensional sum per cluster
    let cluster_sums = result(
	for(rangeiter(0L, k, 1L),
	    appender[vec[f64]],
	    |b,i,e| merge(b, slice(elemwise_sums, e*d, d))
	)
    );

    # On driver
    let new_means = result(for(
	    zip(cluster_sums, cluster_counts),
	    appender[vec[f64]], # note each mean is a d-dimensional point
	    |b,i,e| merge(b, result(for(e.$0,
			appender[f64],
			|b2, i2, e2| merge(b2, e2/f64(e.$1))) # divide each element of mean by cluster count
		))
	)
    );

    let updated_idx = z.$1 + 1L;

    # update means and break if done
    if (updated_idx < iters, {{new_means, updated_idx}, true}, {{new_means, updated_idx}, false})
);

res.$0)WELD";
}

void experiments::kmeans() {
    std::vector<std::pair<size_t, size_t>> edges;

    std::ifstream ifstream("/data/taxi-kmeans-data.csv");
    size_t numPoints;
    ifstream >> numPoints;
    weld_vector<weld_vector<double>> x;
    x.length = numPoints;
    x.data = new weld_vector<double>[numPoints];
    weld_vector<weld_vector<double>> means;
    means.length = 30;
    means.data = new weld_vector<double>[30];

    for (size_t i = 0; i < numPoints; i++) {
        size_t idx;
        double lon, lat, lucrat;
        char c;
        ifstream >> idx >> c >> lon >> c >> lat >> lucrat;
        auto &currPoint = x.data[i];
        currPoint.length = 2;
        currPoint.data = new double[3];
        currPoint.data[0] = lon;
        currPoint.data[1] = lat;
        if (i < 30) {
            auto &currMeans = means.data[i];
            currMeans.data = new double[3];
            currMeans.length = 2;
            currMeans.data[0] = lon;
            currMeans.data[1] = lat;
        }
    }
    k_means_args args;
    args.data_vec = x;
    args.k = 30;
    args.iters = 1;
    args.means = means;



    // Compile Weld module.
    weld_error_t e = weld_error_new();
    weld_conf_t conf = weld_conf_new();
    weld_conf_set(conf, "weld.compile.dumpCode", "true");

    struct timeval start, end, diff;
    gettimeofday(&start, 0);
    weld_module_t m = weld_module_compile(kmeans_code.c_str(), conf, e);
    weld_conf_free(conf);
    gettimeofday(&end, 0);
    timersub(&end, &start, &diff);
    printf("Weld compile time: %ld.%06ld\n",
           (long) diff.tv_sec, (long) diff.tv_usec);

    if (weld_error_code(e)) {
        const char *err = weld_error_message(e);
        printf("Error message: %s\n", err);
        exit(1);
    }



    // Run the module and get the result.
    conf = weld_conf_new();
    weld_conf_set(conf, "weld.threads", "16");
    std::cout << "threads:"<<weld_conf_get(conf, "weld.threads") << std::endl;
    weld_value_t weld_args = weld_value_new(&args);
    void *res2;
    //for (size_t i = 0; i < 100; i++) {
    gettimeofday(&start, 0);
    weld_value_t result = weld_module_run(m, conf, weld_args, e);
    res2 = weld_value_data(result);

    //}
    if (weld_error_code(e)) {
        const char *err = weld_error_message(e);
        printf("Error message: %s\n", err);
        exit(1);
    }


    gettimeofday(&end, 0);
    timersub(&end, &start, &diff);
    printf("Weld: %ld.%06ld\n",
           (long) diff.tv_sec, (long) diff.tv_usec);
}