#pragma once
#ifndef _DEF_H_
#define _DEF_H_

#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include <queue>
#include <fstream>
#include <future>
#include <unordered_set>
#include <unordered_map>
#include <chrono>
#include "omp.h"
using namespace std;


#define ALGORITHM_NESTED_LOOPS                                     1
#define ALGORITHM_SORT_MERGEJOIN                                   2
#define ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP                    3
#define ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING           4
#define ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING 5

#define PROCESSING_SINGLE_THREADED                                 0
#define PROCESSING_PARALLEL_HASH_BASED                             1
#define PROCESSING_PARALLEL_DOMAIN_BASED                           2

#define MINIJOIN_ORIGINALS_ORIGINALS                               0
#define MINIJOIN_REPLICAS_ORIGINALS                                1
#define MINIJOIN_ORIGINALS_REPLICAS                                2
#define MINIJOIN_FREPLICAS_ORIGINALS                               3
#define MINIJOIN_ORIGINALS_FREPLICAS                               4

typedef unsigned long long Timestamp;


class Record;
class Relation;


class Timer
{
private:
    using Clock = std::chrono::high_resolution_clock;
    
    Clock::time_point start_time, stop_time;
    
public:
    Timer()
    {
        start();
    }
    
    void start()
    {
        start_time = Clock::now();
    }
    
    
    double getElapsedTimeInSeconds()
    {
        return std::chrono::duration<double>(stop_time - start_time).count();
    }
    
    
    double stop()
    {
        stop_time = Clock::now();
        return getElapsedTimeInSeconds();
    }
};
#endif
