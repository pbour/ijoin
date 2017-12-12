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


#define ALGORITHM_SORT_MERGEJOIN                                   1
#define ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP                    2
#define ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING           3
#define ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING 4

#define PARALLEL_HASH_BASED                                        1
#define PARALLEL_DOMAIN_BASED                                      2


//#include <iomanip>
typedef unsigned long long Timestamp;
typedef size_t RecordId;

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
