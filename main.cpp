#include "def.h"
#include "getopt.h"
#include "./containers/relation.h"
#include "./containers/bucket_index.h"



// Single-threaded processing
unsigned long long ForwardScanBased_PlaneSweep_Rolled                     (Relation &R, Relation &S);
unsigned long long ForwardScanBased_PlaneSweep_Unrolled                   (Relation &R, Relation &S);
unsigned long long ForwardScanBased_PlaneSweep_Grouping_Rolled            (Relation &R, Relation &S);
unsigned long long ForwardScanBased_PlaneSweep_Grouping_Unrolled          (Relation &R, Relation &S);
unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled  (Relation &R, Relation &S, const BucketIndex &BIR, const BucketIndex &BIS);
unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(Relation &R, Relation &S, const BucketIndex &BIR, const BucketIndex &BIS);

// Parallel processing
void ParallelHashBased_Partitioning(const Relation &R, const Relation &S, Relation *pR, Relation *pS, int runNumThreads);
void ParallelHashBased_Partitioning(const Relation& R, const Relation& S, Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumThreads, int runNumBuckets);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Rolled                     (Relation *pR, Relation *pS, int runNumThreads);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Unrolled                   (Relation *pR, Relation *pS, int runNumThreads);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Rolled            (Relation *pR, Relation *pS, int runNumThreads);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Unrolled          (Relation *pR, Relation *pS, int runNumThreads);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled  (Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumThreads);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumThreads);



void toLowercase(char *buf)
{
    auto i = 0;
    while (buf[i])
    {
        buf[i] = tolower(buf[i]);
        i++;
    }
}


void usage()
{
    cerr << "NAME" << endl;
    cerr << "       ij - compute interval overlap join" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ij [OPTION]... [FILE1] [FILE2]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       Mandatory arguments" << endl << endl;
    cerr << "       -a" << endl;
    cerr << "              join algorithm" << endl;
    cerr << "       -s" << endl;
    cerr << "              pre-sort input files; mandatory only for single-threaded processing" << endl;
    cerr << "       -b" << endl;
    cerr << "              number of buckets; default value is 1000; mandatory only for bgFS algorithm" << endl;
    cerr << "       -h" << endl;
    cerr << "              use hash-based partitioning for parallel processing" << endl;
    cerr << "       -d" << endl;
    cerr << "              use domain-based partitioning for parallel processing" << endl;
    cerr << "       -t" << endl;
    cerr << "              number of threads available" << endl << endl;
    cerr << "       Other arguments" << endl << endl;
    cerr << "       -u" << endl;
    cerr << "              loop unrolling enforced; by default is deactivated" << endl;
    cerr << "       -g" << endl;
    cerr << "              greedy scheduling activated; valid only with -d option" << endl;
    cerr << "       -m" << endl;
    cerr << "              mini-joins breakdown activated; valid only with -d option" << endl;
    cerr << "       -v" << endl;
    cerr << "              adaptive partitioning activated; valid only with -d option" << endl;
    cerr << "       -?" << endl;
    cerr << "              display this help message and exit" << endl << endl;
    exit(1);
}


void report(const char *file1, const char *file2, unsigned long long result, double timeSorting, double timeIndexingOrPartitioning, double timeJoining, int runAlgorithm, int runParallel, int runNumThreads, int runNumBuckets, bool runUnrolled)
{
    stringstream ss;

    
    cout << "R           : " << file1 << endl;
    cout << "S           : " << file2 << endl;
    
    switch (runAlgorithm)
    {
        case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP:
            ss << "FS";
            break;
        case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING:
            ss << "gFS";
            break;
        case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING:
            ss << "bgFS" << endl << "Buckets     : " << runNumBuckets;
            break;
    }
    cout << "Algorithm   : " << ss.str() << endl;
    cout << "Unrolling   : " << ((runUnrolled) ? "yes": "no") << endl;

    ss.str("");
    switch (runParallel)
    {
        case 0:
            ss << "Single-threaded";
            break;
        case PARALLEL_HASH_BASED:
            ss << "Parallel, hash-based" << endl << "Threads     : " << runNumThreads;
            break;
        case PARALLEL_DOMAIN_BASED:
            ss << "Parallel, domain-based" << endl << "Threads     : " << runNumThreads;
            break;
    }
    cout << "Processing  : " << ss.str() << endl;
    cout << "Result      : " << result << endl;
    cout << "Sorting     : " << timeSorting << " secs" << endl;
    
    if (runParallel > 0)
    {
        cout << "Partitioning: ";
    }
    else if (runAlgorithm == ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING)
    {
        cout << "Indexing    : ";
    }
    cout << timeIndexingOrPartitioning << " secs" << endl;
    cout << "Joining     : " << timeJoining << " secs" << endl;

}


int main(int argc, char **argv)
{
    char c;
    unsigned int runAlgorithm = 0, runNumBuckets = 1000, runParallel = 0, runNumThreads = 1, n;
    bool runPresorted = false, runUnrolled = false, runGreedyScheduling = false, runMiniJoinsBreakDown = false, runAdaptivePartitioning = false;
    Timer tim;
    double timeSorting = 0, timeIndexingOrPartitioning = 0, timeJoining = 0;
    Relation R, S, *pR, *pS, *prR, *prS, *prfR, *prfS;
    size_t *pR_size, *pS_size, *prR_size, *prS_size, *prfR_size, *prfS_size;
    BucketIndex BIR, BIS, *pBIR, *pBIS, *prBIR, *prBIS;
    unsigned long long result = 0;


    // Parse command line input.
    while ((c = getopt(argc, argv, "a:sb:hdt:ugmv?")) != -1)
    {
        switch (c)
        {
            case 'a':
                toLowercase(optarg);
                if (strcmp(optarg, "smj") == 0)
                    runAlgorithm = ALGORITHM_SORT_MERGEJOIN;
                else if (strcmp(optarg, "fs") == 0)
                    runAlgorithm = ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP;
                else if (strcmp(optarg, "gfs") == 0)
                    runAlgorithm = ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING;
                else if (strcmp(optarg, "bgfs") == 0)
                    runAlgorithm = ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING;
                else
                {
                    cerr << "error - unknown algorithm '" << optarg << "'" << endl;
                    usage();
                }
                break;
            case 's':
                runPresorted = true;
                break;
            case 'b':
                runNumBuckets = atoi(optarg);
                break;
            case 'h':
                runParallel = PARALLEL_HASH_BASED;
                break;
            case 'd':
                runParallel = PARALLEL_DOMAIN_BASED;
                break;
            case 't':
                runNumThreads = atoi(optarg);
                n = sqrt(runNumThreads);
                break;
            case 'u':
                runUnrolled = true;
                break;
            case 'g':
                runGreedyScheduling = true;
                break;
            case 'm':
                runMiniJoinsBreakDown = true;
                break;
            case 'v':
                runAdaptivePartitioning = true;
                break;
            case '?':
                usage();
            default:
                usage();
        }
    }
    
    // Sanity check
    if (argc-optind < 2)
    {
        cerr << "error - input files not specified" << endl;
        return 1;
    }
    if ((runAlgorithm == ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING) &&  (runNumBuckets < 1))
    {
        cerr << "error - number of buckets should be at least 1" << endl;
        return 1;
    }
    if ((!runParallel) && (!runPresorted))
    {
        cerr << "error - mandatory option '-s' for single-threaded processing" << endl;
        return 1;
    }
    if ((runParallel ==  PARALLEL_DOMAIN_BASED) && (runGreedyScheduling) && (runMiniJoinsBreakDown))
    {
        cerr << "error - greedy scheduling can only be used with mini-joins break down" << endl;
        return 1;
    }
    
    
    // Load inputs
    #pragma omp parallel sections
    {
        #pragma omp section
        {
            R.load(argv[optind]);
        }
        #pragma omp section
        {
            S.load(argv[optind+1]);
        }
    }

    
    // Sort inputs, if needed.
    if (!runParallel)
    {
        tim.start();
        R.sortByStart();
        S.sortByStart();
        timeSorting = tim.stop();
    }
    else if ((runParallel > 0) && (runPresorted))
    {
        tim.start();
        #pragma omp parallel sections
        {
            #pragma omp section
            {
                R.sortByStart();
            }
            #pragma omp section
            {
                S.sortByStart();
            }
        }
        timeSorting = tim.stop();
    }
    
    
    // Compute join
    switch (runAlgorithm)
    {
        case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP:
            if (!runParallel)
            {
                if (runUnrolled)
                {
                    tim.start();
                    result = ForwardScanBased_PlaneSweep_Unrolled(R, S);
                    timeJoining = tim.stop();
                }
                else
                {
                    tim.start();
                    result = ForwardScanBased_PlaneSweep_Rolled(R, S);
                    timeJoining = tim.stop();
                }
            }
            else if (runParallel == PARALLEL_HASH_BASED)
            {
                pR = new Relation[n];
                pS = new Relation[n];

                tim.start();
                ParallelHashBased_Partitioning(R, S, pR, pS, runNumThreads);
                timeIndexingOrPartitioning = tim.stop();
                
                if (runUnrolled)
                {
                    tim.start();
                    result = ParallelHashBased_ForwardScanBased_PlaneSweep_Unrolled(pR, pS, runNumThreads);
                    timeJoining = tim.stop();
                }
                else
                {
                    tim.start();
                    result = ParallelHashBased_ForwardScanBased_PlaneSweep_Rolled(pR, pS, runNumThreads);
                    timeJoining = tim.stop();
                }

                delete[] pR;
                delete[] pS;
            }
            break;

        case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING:
            if (!runParallel)
            {
                if (runUnrolled)
                {
                    tim.start();
                    result = ForwardScanBased_PlaneSweep_Grouping_Unrolled(R, S);
                    timeJoining = tim.stop();
                }
                else
                {
                    tim.start();
                    result = ForwardScanBased_PlaneSweep_Grouping_Rolled(R, S);
                    timeJoining = tim.stop();
                }
            }
            else if (runParallel == PARALLEL_HASH_BASED)
            {
                pR = new Relation[n];
                pS = new Relation[n];

                tim.start();
                ParallelHashBased_Partitioning(R, S, pR, pS, runNumThreads);
                timeIndexingOrPartitioning = tim.stop();
                
                if (runUnrolled)
                {
                    tim.start();
                    result = ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Unrolled(pR, pS, runNumThreads);
                    timeJoining = tim.stop();
                }
                else
                {
                    tim.start();
                    result = ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Rolled(pR, pS, runNumThreads);
                    timeJoining = tim.stop();
                }

                delete[] pR;
                delete[] pS;
            }
            break;

        case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING:
            if (!runParallel)
            {
                tim.start();
                BIR.build(R, runNumBuckets);
                BIS.build(S, runNumBuckets);
                timeIndexingOrPartitioning = tim.stop();
                
                if (runUnrolled)
                {
                    tim.start();
                    result = ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(R, S, BIR, BIS);
                    timeJoining = tim.stop();
                }
                else
                {
                    tim.start();
                    result = ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(R, S, BIR, BIS);
                    timeJoining = tim.stop();
                }
            }
            else if (runParallel == PARALLEL_HASH_BASED)
            {
                pR = new Relation[n];
                pS = new Relation[n];
                pBIR = new BucketIndex[n];
                pBIS = new BucketIndex[n];
                
                tim.start();
                ParallelHashBased_Partitioning(R, S, pR, pS, pBIR, pBIS, runNumThreads, runNumBuckets);
                timeIndexingOrPartitioning += tim.stop();
                
                if (runUnrolled)
                {
                    tim.start();
                    result = ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(pR, pS, pBIR, pBIS, runNumThreads);
                    timeJoining = tim.stop();
                }
                else
                {
                    tim.start();
                    result = ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(pR, pS, pBIR, pBIS, runNumThreads);
                    timeJoining = tim.stop();
                }
                
                delete[] pR;
                delete[] pS;
                delete[] pBIR;
                delete[] pBIS;

            }
            break;
    }
    
    
    // Report stats
//    void report(const char *file1, const char *file2, unsigned long long result, double timeSorting, double timeIndexingOrPartitioning, double timeJoining, int runAlgorithm, int runParallel)
    report(argv[optind], argv[optind+1], result, timeSorting, timeIndexingOrPartitioning, timeJoining, runAlgorithm, runParallel, runNumThreads, runNumBuckets, runUnrolled);

    
    return 0;
}
