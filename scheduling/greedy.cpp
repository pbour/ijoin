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


#include "../containers/relation.h"
#include "greedy.h"



MiniJoin::MiniJoin(int partitionId, int type, size_t cost)
{
	this->partitionId = partitionId;
	this->type = type;
	this->cost = cost;
}



ThreadBag::ThreadBag()
{
	this->cost = 0;
}



GreedyScheduler::GreedyScheduler()
{
}


void GreedyScheduler::schedule(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads)
{
	// Initialize thread bags and mini-joins list
	for (size_t i = 0; i < runNumPartitionsPerRelation; i++)
	{
		MJL.push_back(MiniJoin((int)i, MINIJOIN_ORIGINALS_ORIGINALS, (pR[i].size()*pS[i].size())));
		MJL.push_back(MiniJoin((int)i, MINIJOIN_REPLICAS_ORIGINALS,  (prR[i].size()*pS[i].size())));
		MJL.push_back(MiniJoin((int)i, MINIJOIN_ORIGINALS_REPLICAS,  (pR[i].size()*prS[i].size())));
		MJL.push_back(MiniJoin((int)i, MINIJOIN_FREPLICAS_ORIGINALS, (prfR[i].size()*pS[i].size())));
		MJL.push_back(MiniJoin((int)i, MINIJOIN_ORIGINALS_FREPLICAS, (pR[i].size()*prfS[i].size())));
	}
	sort(MJL.begin(), MJL.end());
	
	for (size_t i = 0; i < runNumThreads; i++)
		TBH.push_back(ThreadBag());
	make_heap(TBH.begin(), TBH.end());//, CompareThreadBags());

	for (size_t mj = 0; mj < MJL.size(); mj++)
	{
		if (MJL[mj].cost > 0)
		{
			ThreadBag tb = TBH.front();
			tb.mjoins.push_back(std::pair<int, int>(MJL[mj].partitionId, MJL[mj].type));
			tb.cost += MJL[mj].cost;

			pop_heap(TBH.begin(), TBH.end());//, CompareThreadBags());
			TBH.pop_back();
			TBH.push_back(tb);
			push_heap(TBH.begin(), TBH.end());//, CompareThreadBags());
		}
	}
}
