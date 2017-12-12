#include "../def.h"
#include "../containers/relation.h"
#include "../containers/bucket_index.h"



void ParallelHashBased_Partitioning(const Relation& R, const Relation& S, Relation *pR, Relation *pS, int runNumThreads)
{
    int n = sqrt(runNumThreads);
    
    
    #pragma omp parallel num_threads(2*n)
    {
        #pragma omp single
        {
            for (int j = 0; j < n; j++)
            {
                #pragma omp task
                {
                    pR[j].load(R, j, n);
                }
                
                #pragma omp task
                {
                    pS[j].load(S, j, n);
                }
            }
        }
    }
}


void ParallelHashBased_Partitioning(const Relation& R, const Relation& S, Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumThreads, int runNumBuckets)
{
    int n = sqrt(runNumThreads);
    
    
    #pragma omp parallel num_threads(2*n)
    {
        #pragma omp single
        {
            for (int j = 0; j < n; j++)
            {
                #pragma omp task depend(out: pR[j])
                {
                    pR[j].load(R, j, n);
                }
                
                #pragma omp task depend(out: pS[j])
                {
                    pS[j].load(S, j, n);
                }
            }
            
            for (int j = 0; j < n; j++)
            {
                #pragma omp task depend(in: pR[j])
                {
                    pBIR[j].build(pR[j], runNumBuckets);
                }
                
                #pragma omp task depend(in: pS[j])
                {
                    pBIS[j].build(pS[j], runNumBuckets);
                }
            }
        }
    }
}
