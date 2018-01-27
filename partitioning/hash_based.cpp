#include "../def.h"
#include "../containers/relation.h"
#include "../containers/bucket_index.h"



void ParallelHashBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, int runNumPartitionsPerRelation, int runNumThreads)
{
	#pragma omp parallel num_threads(2*runNumPartitionsPerRelation)
	{
		#pragma omp single
		{
			for (int p = 0; p < runNumPartitionsPerRelation; p++)
			{
				#pragma omp task
				{
					pR[p].load(R, p, runNumPartitionsPerRelation);
				}
				
				#pragma omp task
				{
					pS[p].load(S, p, runNumPartitionsPerRelation);
				}
			}
		}
	}
}


void ParallelHashBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads, int runNumBuckets)
{
	#pragma omp parallel num_threads(2*runNumPartitionsPerRelation)
	{
		#pragma omp single
		{
			for (int p = 0; p < runNumPartitionsPerRelation; p++)
			{
				#pragma omp task depend(out: pR[p])
				{
                    pR[p].load(R, p, runNumPartitionsPerRelation);
				}
				
				#pragma omp task depend(out: pS[p])
				{
					pS[p].load(S, p, runNumPartitionsPerRelation);
				}
			}
			
			for (int p = 0; p < runNumPartitionsPerRelation; p++)
			{
				#pragma omp task depend(in: pR[p])
				{
					pBIR[p].build(pR[p], runNumBuckets);
				}
				
				#pragma omp task depend(in: pS[p])
				{
					pBIS[p].build(pS[p], runNumBuckets);
				}
			}
		}
	}
}
