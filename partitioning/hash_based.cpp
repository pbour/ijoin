/******************************************************************************
 * Project:  ijoin
 * Purpose:  Compute interval overlap joins
 * Author:   Panagiotis Bouros, pbour@github.io
 ******************************************************************************
 * Copyright (c) 2017, Panagiotis Bouros
 *
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************/


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
