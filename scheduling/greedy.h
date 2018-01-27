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


#ifndef _GREEDY_H_
#define _GREEDY_H_
#include "../def.h"



class MiniJoin
{
public:
	int partitionId;
	int type;
	size_t cost;
	
	MiniJoin(int partitionId, int type, size_t cost);
	
	bool operator < (const MiniJoin& rhs) const noexcept
	{
		return (this->cost > rhs.cost);
	}
};



class ThreadBag
{
public:
	std::vector<std::pair<int, int> > mjoins;
	size_t cost;
	
	ThreadBag();
	
	bool operator < (const ThreadBag& rhs) const noexcept
	{
		return (this->cost > rhs.cost);
	}
};



class GreedyScheduler
{
public:
	vector<MiniJoin> MJL;
	vector<ThreadBag> TBH;
	
	GreedyScheduler();
	void schedule(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads);
};
#endif // _GREEDY_H_
