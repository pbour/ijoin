#include "def.h"
#include "getopt.h"
#include "./containers/relation.h"
#include "./containers/bucket_index.h"



// Single-threaded processing
unsigned long long ForwardScanBased_PlaneSweep                   (Relation &R, Relation &S, bool runUnrolled);
unsigned long long ForwardScanBased_PlaneSweep_Grouping          (Relation &R, Relation &S, bool runUnrolled);
unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing(Relation &R, Relation &S, const BucketIndex &BIR, const BucketIndex &BIS, bool runUnrolled);

// Hash-based parallel processing
void ParallelHashBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, int runNumPartitionsPerRelation, int runNumThreads);
void ParallelHashBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumBuckets, int runNumPartitionsPerRelation, int runNumThreads);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep                   (Relation *pR, Relation *pS, int runNumPartitionsPerRelation, int runNumThreads, bool runUnrolled);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping          (Relation *pR, Relation *pS, int runNumPartitionsPerRelation, int runNumThreads, bool runUnrolled);
unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing(Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads, bool runUnrolled);

// Domain-based parallel processing
void ParallelDomainBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads, bool runMiniJoinsBreakdown, bool runAdaptivePartitioning);
void ParallelDomainBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *BIR, BucketIndex *BIS, int runNumPartitionsPerRelation, int runNumBuckets, int runNumThreads, bool runMiniJoinsBreakdown, bool runAdaptivePartitioning);
unsigned long long ParallelDomainBased_ForwardScanBased_PlaneSweep(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads, bool runUnrolled, bool runGreedyScheduling, bool runMiniJoinsBreakDown, bool runAdaptivePartitioning);
unsigned long long ParallelDomainBased_ForwardScanBased_PlaneSweep_Grouping(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads, bool runUnrolled, bool runGreedyScheduling, bool runMiniJoinsBreakdown, bool runAdaptivePartitioning);
unsigned long long ParallelDomainBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads, bool runUnrolled, bool runGreedyScheduling, bool runMiniJoinsBreakDown, bool runAdaptivePartitioning);


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
    cerr << "              loop unrolling activated; by default is deactivated" << endl;
    cerr << "       -g" << endl;
    cerr << "              greedy scheduling activated; valid only with -d option" << endl;
    cerr << "       -m" << endl;
    cerr << "              mini-joins breakdown activated; valid only with -d option" << endl;
    cerr << "       -v" << endl;
    cerr << "              adaptive partitioning activated; valid only with -d option" << endl;
    cerr << "       -?" << endl;
    cerr << "              display this help message and exit" << endl << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       Original forward scan-based plane sweep from BrinkhoffKS@SIGMOD96, single-threaded processing" << endl;
    cerr << "              ij -a FS -u -s FILE1 FILE2" << endl;
    cerr << "       Optimized FS with grouping, hash-based parallel processing" << endl;
    cerr << "              ij -a gFS -u -h -t 16 FILE1 FILE2" << endl;
    cerr << "       Optimized FS with grouping and bucket indexing (1000 buckets), domain-based parallel processing under mj+greedy/adaptive setup" << endl;
    cerr << "              ij -a bgFS -b 1000 -u -d -m -g -v -t 16 FILE1 FILE2" << endl << endl;
    exit(1);
}


void report(const char *file1, const char *file2, unsigned long long result, double timeSorting, double timeIndexingOrPartitioning, double timeJoining, int runAlgorithm, int runParallel, int runNumThreads, int runNumBuckets, bool runUnrolled, bool runPresorted, bool runGreedyScheduling, bool runMiniJoinsBreakdown, bool runAdaptivePartitioning)
{
    stringstream ss;

    
    cout << "R                    : " << file1 << endl;
    cout << "S                    : " << file2 << endl;
    
    switch (runAlgorithm)
    {
        case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP:
            ss << "FS";
            break;
        case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING:
            ss << "gFS";
            break;
        case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING:
            ss << "bgFS" << endl << "Buckets              : " << runNumBuckets;
            break;
    }
    cout << "Algorithm            : " << ss.str() << endl;
    cout << "Loop unrolling       : " << ((runUnrolled) ? "yes": "no") << endl;

    ss.str("");
    switch (runParallel)
    {
        case 0:
            ss << "Single-threaded";
            break;
        case PROCESSING_PARALLEL_HASH_BASED:
            ss << "Parallel, hash-based" << endl << "Threads              : " << runNumThreads;
            break;
        case PROCESSING_PARALLEL_DOMAIN_BASED:
            ss << "Parallel, domain-based" << endl << "Threads              : " << runNumThreads << endl << "Greedy scheduling    : " << ((runGreedyScheduling) ? "yes": "no") << endl << "Mini-joins breakdown : " << ((runMiniJoinsBreakdown) ? "yes": "no") << endl << "Adaptive partitioning: " << ((runAdaptivePartitioning) ? "yes": "no");
            break;
    }
    cout << "Processing           : " << ss.str() << endl;
    cout << "Result (XOR start)   : " << result << endl;
    
    if (runPresorted)
        cout << "Sorting time         : " << timeSorting << " secs" << endl;
    
    if (runParallel > 0)
    {
        cout << "Partitioning time    : " << timeIndexingOrPartitioning << " secs" << endl;
    }
    else if (runAlgorithm == ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING)
    {
        cout << "Indexing time        : " << timeIndexingOrPartitioning << " secs" << endl;
    }
    cout << "Joining time         : " << timeJoining << " secs" << endl;
    cout << "Total time           : " << (timeSorting+timeIndexingOrPartitioning+timeJoining) << " secs" << endl << endl;
}


int main(int argc, char **argv)
{
    char c;
    unsigned int runAlgorithm = 0, runNumBuckets = 1000, runParallel = PROCESSING_SINGLE_THREADED, runNumThreads = 1, runNumPartitionsPerRelation;
    bool runPresorted = false, runUnrolled = false, runGreedyScheduling = false, runMiniJoinsBreakdown = false, runAdaptivePartitioning = false;
    Timer tim;
    double timeSorting = 0, timeIndexingOrPartitioning = 0, timeJoining = 0;
    Relation R, S, *pR, *pS, *prR, *prS, *prfR, *prfS;
//    size_t *pR_size, *pS_size, *prR_size, *prS_size, *prfR_size, *prfS_size;
    BucketIndex BIR, BIS, *pBIR, *pBIS;
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
                runParallel = PROCESSING_PARALLEL_HASH_BASED;
                break;
            case 'd':
                runParallel = PROCESSING_PARALLEL_DOMAIN_BASED;
                break;
            case 't':
                runNumThreads = atoi(optarg);
                break;
            case 'u':
                runUnrolled = true;
                break;
            case 'g':
                runGreedyScheduling = true;
                break;
            case 'm':
                runMiniJoinsBreakdown = true;
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
    if ((runParallel ==  PROCESSING_PARALLEL_DOMAIN_BASED) && (runGreedyScheduling) && (!runMiniJoinsBreakdown))
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
    

    // Compute join.
    switch (runParallel)
    {
        case PROCESSING_SINGLE_THREADED:
            switch (runAlgorithm)
            {
                case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP:
                    tim.start();
                    result = ForwardScanBased_PlaneSweep(R, S, runUnrolled);
                    timeJoining = tim.stop();
                    break;
                    
                case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING:
                    tim.start();
                    result = ForwardScanBased_PlaneSweep_Grouping(R, S, runUnrolled);
                    timeJoining = tim.stop();
                    break;
                
                case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING:
                    tim.start();
                    BIR.build(R, runNumBuckets);
                    BIS.build(S, runNumBuckets);
                    timeIndexingOrPartitioning = tim.stop();

                    tim.start();
                    result = ForwardScanBased_PlaneSweep_Grouping_Bucketing(R, S, BIR, BIS, runUnrolled);
                    timeJoining = tim.stop();
                    break;
            }
            break;

        case PROCESSING_PARALLEL_HASH_BASED:
            // Partitions for inputs R and S.
            runNumPartitionsPerRelation = sqrt(runNumThreads);
            pR = new Relation[runNumPartitionsPerRelation];
            pS = new Relation[runNumPartitionsPerRelation];
            switch (runAlgorithm)
            {
                case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP:
                    tim.start();
                    ParallelHashBased_Partition(R, S, pR, pS, runNumPartitionsPerRelation, runNumThreads);
                    timeIndexingOrPartitioning = tim.stop();
                    
                    tim.start();
                    result = ParallelHashBased_ForwardScanBased_PlaneSweep(pR, pS, runNumPartitionsPerRelation, runNumThreads, runUnrolled);
                    timeJoining = tim.stop();
                    break;
                
                case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING:
                    tim.start();
                    ParallelHashBased_Partition(R, S, pR, pS, runNumPartitionsPerRelation, runNumThreads);
                    timeIndexingOrPartitioning = tim.stop();
                    
                    tim.start();
                    result = ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping(pR, pS, runNumPartitionsPerRelation, runNumThreads, runUnrolled);
                    timeJoining = tim.stop();
                    break;
                
                case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING:
                    // Bucket index for inputs R and S.
                    pBIR = new BucketIndex[runNumPartitionsPerRelation];
                    pBIS = new BucketIndex[runNumPartitionsPerRelation];
                    
                    tim.start();
                    ParallelHashBased_Partition(R, S, pR, pS, pBIR, pBIS, runNumBuckets, runNumPartitionsPerRelation, runNumThreads);
                    timeIndexingOrPartitioning = tim.stop();
                    
                    tim.start();
                    result = ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing(pR, pS, pBIR, pBIS, runNumPartitionsPerRelation, runNumThreads, runUnrolled);
                    timeJoining = tim.stop();
                    
                    delete[] pBIR;
                    delete[] pBIS;
                    break;
            }
            delete[] pR;
            delete[] pS;
            break;
            
        case PROCESSING_PARALLEL_DOMAIN_BASED:
            // Partitions for inputs R and S: pR (pS) original records (type (i)), prR (prS) replicas (type (ii)), prfR (prfS) for replicas covering the entire partition (type (iii)).
            runNumPartitionsPerRelation = runNumThreads;
            pR = new Relation[runNumPartitionsPerRelation];
            pS = new Relation[runNumPartitionsPerRelation];
            prR = new Relation[runNumPartitionsPerRelation];
            prS = new Relation[runNumPartitionsPerRelation];
            if (runMiniJoinsBreakdown)
            {
                prfR = new Relation[runNumPartitionsPerRelation];
                prfS = new Relation[runNumPartitionsPerRelation];
            }

            switch (runAlgorithm)
            {
                case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP:
                    tim.start();
                    ParallelDomainBased_Partition(R, S, pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads, runMiniJoinsBreakdown, runAdaptivePartitioning);
                    timeIndexingOrPartitioning = tim.stop();
                    
                    tim.start();
                    result = ParallelDomainBased_ForwardScanBased_PlaneSweep(pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads, runUnrolled, runGreedyScheduling, runMiniJoinsBreakdown, runAdaptivePartitioning);
                    timeJoining = tim.stop();
                    break;
                
                case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING:
                    tim.start();
                    ParallelDomainBased_Partition(R, S, pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads, runMiniJoinsBreakdown, runAdaptivePartitioning);
                    timeIndexingOrPartitioning = tim.stop();
                    
                    tim.start();
                    result = ParallelDomainBased_ForwardScanBased_PlaneSweep_Grouping(pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads, runUnrolled, runGreedyScheduling, runMiniJoinsBreakdown, runAdaptivePartitioning);
                    timeJoining = tim.stop();
                    break;
                
                case ALGORITHM_FORWARD_SCAN_BASED_PLANESWEEP_GROUPING_BUCKETING:
                    // Bucket index for inputs R and S.
                    pBIR = new BucketIndex[runNumThreads];
                    pBIS = new BucketIndex[runNumThreads];
                    
                    tim.start();
                    ParallelDomainBased_Partition(R, S, pR, pS, prR, prS, prfR, prfS, pBIR, pBIS, runNumBuckets, runNumPartitionsPerRelation, runNumThreads, runMiniJoinsBreakdown, runAdaptivePartitioning);
                    timeIndexingOrPartitioning = tim.stop();
                    
                    tim.start();
                    result = ParallelDomainBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing(pR, pS, prR, prS, prfR, prfS, pBIR, pBIS, runNumPartitionsPerRelation, runNumThreads, runUnrolled, runGreedyScheduling, runMiniJoinsBreakdown, runAdaptivePartitioning);
                    timeJoining = tim.stop();

                    delete[] pBIR;
                    delete[] pBIS;
                    break;
            }
            delete[] pR;
            delete[] pS;
            delete[] prR;
            delete[] prS;
            if (runMiniJoinsBreakdown)
            {
                delete[] prfR;
                delete[] prfS;
            }
            break;
    }

    
    // Report stats
    report(argv[optind], argv[optind+1], result, timeSorting, timeIndexingOrPartitioning, timeJoining, runAlgorithm, runParallel, runNumThreads, runNumBuckets, runUnrolled, runPresorted, runGreedyScheduling, runMiniJoinsBreakdown, runAdaptivePartitioning);

    
    return 0;
}
