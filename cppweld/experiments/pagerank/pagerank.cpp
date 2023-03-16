#include "weld.h"

#include <iostream>
#include "../experiments.h"
#include <vector>
#include <unordered_map>
#include <cstring>
#include <sys/time.h>
#include <fstream>
#include <sstream>
#include<chrono>

namespace {
    std::string weld_pagerank_prepare = R"WELD(
|in: vec[{i32,i32}], numNodes: i64|
let groupdict = result(for(in, groupmerger[i32,i32], |b,i,e| merge(b, e)));
let adj = result( @(grain_size: 1)for(rangeiter(0L, numNodes, 1L), appender[vec[i32]], |b,i,e| merge(b, lookup(groupdict,i32(e)))));
adj
)WELD";


    std::string weld_pagerank_no_loop = R"WELD(
|in: vec[vec[i32]], old_ranks: vec[f64], outgoing: vec[f64], numNodes: i64|
result(for(in, appender[f64], |b, i, e|
        merge(b, result(for(
                    e,
                    merger[f64,+],
                    |b2,i2,e2| merge(b2, lookup(old_ranks, i64(e2))/lookup(outgoing, i64(e2)))
                ))*0.85+0.15/f64(numNodes)
        )
    ))
)WELD";
    //this would be the complete pagerank algorithm, but Weld can't run it efficiently :(
    std::string weld_pagerank = R"WELD(
|in: vec[vec[i32]], old_ranks: vec[f64], outgoing: vec[f64], numNodes: i64|
let iter_idx = 0L;
let iters=100L;
let res = iterate({old_ranks, iter_idx},
    |z|
    let new_ranks=result(for(in, appender[f64], |b, i, e|
            merge(b, result(for(
                        e,
                        merger[f64,+],
                        |b2,i2,e2| merge(b2, lookup(z.$0, i64(e2))/lookup(outgoing, i2))
                    ))*0.85+0.15/f64(numNodes)
            )
        ));
    let updated_idx = z.$1 + 1L;
    if (updated_idx < iters, {{new_ranks, updated_idx}, true}, {{new_ranks, updated_idx}, false})
);
res.$0
)WELD";
    template<typename T>
    struct t_weld_vector {
        T *data;
        int64_t length;
    };

    typedef struct parse_output {
        t_weld_vector<t_weld_vector<int32_t>> outlinks;
    } parse_output;

    typedef struct pagerank_args {
        t_weld_vector<t_weld_vector<int32_t>> in;
        t_weld_vector<double> ranks;
        t_weld_vector<double> outgoing;
        uint64_t numNodes;
    } pagerank_args;
    struct prepare_args {
        struct p {
            uint64_t a;
            uint64_t b;
        };
        t_weld_vector<p> in;
        uint64_t numNodes;
    };
    struct prepare_res {
        t_weld_vector<t_weld_vector<int32_t>> graph_data;

    };
}

void experiments::pagerank() {
    std::vector<std::pair<size_t, size_t>> edges;
    std::unordered_map<size_t, size_t> uniqueIds;
    size_t currId = 0;
    auto getNodeId = [&](size_t pid) {
        if (uniqueIds.count(pid)) {
            return uniqueIds[pid];
        } else {
            size_t nodeId = currId++;
            uniqueIds[pid] = nodeId;
        }
    };
    std::ifstream ifstream("/data/imdb-pagerank-data.csv");
    size_t numEdges;
    ifstream >> numEdges;
    std::cout << "numEdges:" << numEdges << std::endl;
    for (size_t i = 0; i < numEdges; i++) {
        size_t from, to;
        size_t idx;
        char c;
        ifstream >> idx >> c >> from >> c >> to;
        edges.emplace_back(getNodeId(to), getNodeId(from));
    }


    std::unordered_map<size_t, std::vector<size_t>> node_lists;
    std::unordered_map<size_t, size_t> outgoing_count;
    size_t nodes = currId;
    prepare_args prepareArgs;
    prepareArgs.numNodes = nodes;
    prepareArgs.in.length = edges.size();
    prepareArgs.in.data = new prepare_args::p[edges.size()];
    for (size_t i = 0; i < edges.size(); i++) {
        auto e = edges[i];
        node_lists[e.first].push_back(e.second);
        prepareArgs.in.data[i] = {e.first, e.second};
        outgoing_count[e.second]++;
    }

    pagerank_args pargs;
    using timepoint=typeof(std::chrono::steady_clock::now());
    timepoint startPrep,endPrep;
    {
        weld_error_t e = weld_error_new();
        weld_conf_t conf = weld_conf_new();
        auto startCompile = std::chrono::steady_clock::now();

        weld_module_t m = weld_module_compile(weld_pagerank_prepare.c_str(), conf, e);
        weld_conf_free(conf);

        if (weld_error_code(e)) {
            const char *err = weld_error_message(e);
            printf("Error message: %s\n", err);
            exit(1);
        }
        auto endCompile = std::chrono::steady_clock::now();

        // Run the module and get the result.
        conf = weld_conf_new();
        weld_conf_set(conf, "weld.threads", "32");

        weld_value_t weld_args = weld_value_new(&prepareArgs);
        startPrep = std::chrono::steady_clock::now();

        weld_value_t result = weld_module_run(m, conf, weld_args, e);
        auto prepareRes = reinterpret_cast<prepare_res *>(weld_value_data(result));
        pargs.in = prepareRes->graph_data;
        endPrep = std::chrono::steady_clock::now();

    }
    std::cout << "nodes:" << nodes << std::endl;

    pargs.ranks.data = (double *) malloc(sizeof(double) * nodes);
    pargs.ranks.length = nodes;
    pargs.outgoing.data = (double *) malloc(sizeof(double) * nodes);
    pargs.outgoing.length = nodes;
    pargs.numNodes = nodes;
    for (int32_t i = 0; i < nodes; i++) pargs.ranks.data[i] = 1.0 / nodes;
    for (int32_t i = 0; i < nodes; i++) pargs.outgoing.data[i] = outgoing_count[i];// node_lists[i].size();




    // Compile Weld module.
    weld_error_t e = weld_error_new();
    weld_conf_t conf = weld_conf_new();
    auto startCompile = std::chrono::steady_clock::now();

    weld_module_t m = weld_module_compile(weld_pagerank_no_loop.c_str(), conf, e);
    weld_conf_free(conf);

    if (weld_error_code(e)) {
        const char *err = weld_error_message(e);
        printf("Error message: %s\n", err);
        exit(1);
    }
    auto endCompile = std::chrono::steady_clock::now();

    // Run the module and get the result.
    conf = weld_conf_new();
    weld_conf_set(conf, "weld.threads", "32");
    std::cout << "threads:" << weld_conf_get(conf, "weld.threads") << std::endl;
    auto startExecution = std::chrono::steady_clock::now();
    weld_value_t weld_args = weld_value_new(&pargs);

    void *res2;
    if (true) {
        for (size_t i = 0; i < 100; i++) {
            weld_value_t result = weld_module_run(m, conf, weld_args, e);

            res2 = weld_value_data(result);

            pargs.ranks = *(t_weld_vector<double> *) (res2);
        }
    } else {
        weld_value_t result = weld_module_run(m, conf, weld_args, e);

        res2 = weld_value_data(result);
    }

    auto endExecution = std::chrono::steady_clock::now();
    for (size_t i = 0; i < 100; i++) {
        std::cout << pargs.ranks.data[i] << std::endl;

    }
    if (weld_error_code(e)) {
        const char *err = weld_error_message(e);
        printf("Error message: %s\n", err);
        exit(1);
    }
    auto diff = [](auto start, auto end) {
        return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000.0;
    };
    std::cout << "prep: " << diff(startPrep, endPrep) << " compile: " << diff(startCompile, endCompile)
              << " execution: " << diff(startExecution, endExecution) << std::endl;
}