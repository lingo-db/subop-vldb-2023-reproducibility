#include <iostream>
#include "experiments/experiments.h"

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " experiment-name" << std::endl;
        return 1;
    }
    std::string experimentName = argv[1];
    if (experimentName == "pagerank") {
        experiments::pagerank();
    } else if (experimentName == "kmeans") {
        experiments::kmeans();
    } else {
        std::cerr << "unknown experiment" << std::endl;
        return 1;
    }
    return 0;
}