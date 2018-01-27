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
#include "../scheduling/greedy.h"



unsigned long long NestedLoops_Rolled(const Relation &R, const Relation &S);
unsigned long long NestedLoops_Unrolled(const Relation &R, const Relation &S);


////////////////////
// Internal loops //
////////////////////
inline unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing_InternalLoop_Rolled(Group &G, RelationIterator firstFS, RelationIterator lastFS, const BucketIndex &BI, Timestamp minStart)
{
	unsigned long long result = 0;
	int cbucket_id, pbucket_id;


	auto pivot = firstFS;
	auto lastG = G.end();
	for (auto curr = G.begin(); curr != lastG; curr++)
	{
		if (pivot == lastFS)
			break;
		if (curr->end < minStart)
			continue;
		
		cbucket_id = ceil((double)(curr->end-minStart)/BI.bucket_range);
		if (cbucket_id >= BI.numBuckets)
			cbucket_id = BI.numBuckets-1;
		pbucket_id = ceil((double)(pivot->end-minStart)/BI.bucket_range);
		if (pbucket_id >= BI.numBuckets)
			pbucket_id = BI.numBuckets-1;
		
		if (cbucket_id > pbucket_id)
		{
			auto last = BI[cbucket_id-1].last;
			while (pivot != last)
			{
				for (auto k = curr; k != lastG; k++)
				{
					result += k->start ^ pivot->start;
				}
				pivot++;
			}
		}
		
		// Sweep the last bucket.
		auto last = BI[cbucket_id].last;
		while ((pivot != last) && (curr->end >= pivot->start))
		{
			for (auto k = curr; k != lastG; k++)
			{
				result += k->start ^ pivot->start;
			}
			pivot++;
		}
	}

	
	return result;
}


inline unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing_InternalLoop_Unrolled(Group &G, RelationIterator firstFS, RelationIterator lastFS, const BucketIndex &BI, Timestamp minStart)
{
	unsigned long long result = 0;
	int cbucket_id, pbucket_id;
	
	
	auto pivot = firstFS;
	auto lastG = G.end();
	for (auto curr = G.begin(); curr != lastG; curr++)
	{
		auto bufferSize = (lastG-curr);
		
		if (pivot == lastFS)
			break;
		if (curr->end < minStart)
			continue;
		
		cbucket_id = ceil((double)(curr->end-minStart)/BI.bucket_range);
		if (cbucket_id >= BI.numBuckets)
			cbucket_id = BI.numBuckets-1;
		pbucket_id = ceil((double)(pivot->end-minStart)/BI.bucket_range);
		if (pbucket_id >= BI.numBuckets)
			pbucket_id = BI.numBuckets-1;
		
		if (cbucket_id > pbucket_id)
		{
			auto last = BI[cbucket_id-1].last;
			switch (bufferSize)
			{
				case 1:
					while (last-pivot >= 32)
					{
						result += curr->start ^ (pivot+0)->start;
						result += curr->start ^ (pivot+1)->start;
						result += curr->start ^ (pivot+2)->start;
						result += curr->start ^ (pivot+3)->start;
						result += curr->start ^ (pivot+4)->start;
						result += curr->start ^ (pivot+5)->start;
						result += curr->start ^ (pivot+6)->start;
						result += curr->start ^ (pivot+7)->start;
						result += curr->start ^ (pivot+8)->start;
						result += curr->start ^ (pivot+9)->start;
						result += curr->start ^ (pivot+10)->start;
						result += curr->start ^ (pivot+11)->start;
						result += curr->start ^ (pivot+12)->start;
						result += curr->start ^ (pivot+13)->start;
						result += curr->start ^ (pivot+14)->start;
						result += curr->start ^ (pivot+15)->start;
						result += curr->start ^ (pivot+16)->start;
						result += curr->start ^ (pivot+17)->start;
						result += curr->start ^ (pivot+18)->start;
						result += curr->start ^ (pivot+19)->start;
						result += curr->start ^ (pivot+20)->start;
						result += curr->start ^ (pivot+21)->start;
						result += curr->start ^ (pivot+22)->start;
						result += curr->start ^ (pivot+23)->start;
						result += curr->start ^ (pivot+24)->start;
						result += curr->start ^ (pivot+25)->start;
						result += curr->start ^ (pivot+26)->start;
						result += curr->start ^ (pivot+27)->start;
						result += curr->start ^ (pivot+28)->start;
						result += curr->start ^ (pivot+29)->start;
						result += curr->start ^ (pivot+30)->start;
						result += curr->start ^ (pivot+31)->start;
						pivot += 32;
					}
					
					while (pivot < last)
					{
						result += curr->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 2:
					while (last-pivot >= 16)
					{
						result += (curr+0)->start ^ (pivot+0)->start;
						result += (curr+1)->start ^ (pivot+0)->start;
						result += (curr+0)->start ^ (pivot+1)->start;
						result += (curr+1)->start ^ (pivot+1)->start;
						result += (curr+0)->start ^ (pivot+2)->start;
						result += (curr+1)->start ^ (pivot+2)->start;
						result += (curr+0)->start ^ (pivot+3)->start;
						result += (curr+1)->start ^ (pivot+3)->start;
						result += (curr+0)->start ^ (pivot+4)->start;
						result += (curr+1)->start ^ (pivot+4)->start;
						result += (curr+0)->start ^ (pivot+5)->start;
						result += (curr+1)->start ^ (pivot+5)->start;
						result += (curr+0)->start ^ (pivot+6)->start;
						result += (curr+1)->start ^ (pivot+6)->start;
						result += (curr+0)->start ^ (pivot+7)->start;
						result += (curr+1)->start ^ (pivot+7)->start;
						result += (curr+0)->start ^ (pivot+8)->start;
						result += (curr+1)->start ^ (pivot+8)->start;
						result += (curr+0)->start ^ (pivot+9)->start;
						result += (curr+1)->start ^ (pivot+9)->start;
						result += (curr+0)->start ^ (pivot+10)->start;
						result += (curr+1)->start ^ (pivot+10)->start;
						result += (curr+0)->start ^ (pivot+11)->start;
						result += (curr+1)->start ^ (pivot+11)->start;
						result += (curr+0)->start ^ (pivot+12)->start;
						result += (curr+1)->start ^ (pivot+12)->start;
						result += (curr+0)->start ^ (pivot+13)->start;
						result += (curr+1)->start ^ (pivot+13)->start;
						result += (curr+0)->start ^ (pivot+14)->start;
						result += (curr+1)->start ^ (pivot+14)->start;
						result += (curr+0)->start ^ (pivot+15)->start;
						result += (curr+1)->start ^ (pivot+15)->start;
						pivot += 16;
					}
					
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 3:
					while (last-pivot >= 8)
					{
						result += (curr+0)->start ^ (pivot+0)->start;
						result += (curr+1)->start ^ (pivot+0)->start;
						result += (curr+2)->start ^ (pivot+0)->start;
						result += (curr+0)->start ^ (pivot+1)->start;
						result += (curr+1)->start ^ (pivot+1)->start;
						result += (curr+2)->start ^ (pivot+1)->start;
						result += (curr+0)->start ^ (pivot+2)->start;
						result += (curr+1)->start ^ (pivot+2)->start;
						result += (curr+2)->start ^ (pivot+2)->start;
						result += (curr+0)->start ^ (pivot+3)->start;
						result += (curr+1)->start ^ (pivot+3)->start;
						result += (curr+2)->start ^ (pivot+3)->start;
						result += (curr+0)->start ^ (pivot+4)->start;
						result += (curr+1)->start ^ (pivot+4)->start;
						result += (curr+2)->start ^ (pivot+4)->start;
						result += (curr+0)->start ^ (pivot+5)->start;
						result += (curr+1)->start ^ (pivot+5)->start;
						result += (curr+2)->start ^ (pivot+5)->start;
						result += (curr+0)->start ^ (pivot+6)->start;
						result += (curr+1)->start ^ (pivot+6)->start;
						result += (curr+2)->start ^ (pivot+6)->start;
						result += (curr+0)->start ^ (pivot+7)->start;
						result += (curr+1)->start ^ (pivot+7)->start;
						result += (curr+2)->start ^ (pivot+7)->start;
						pivot += 8;
					}
					
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 4:
					while (last-pivot >= 8)
					{
						result += (curr+0)->start ^ (pivot+0)->start;
						result += (curr+1)->start ^ (pivot+0)->start;
						result += (curr+2)->start ^ (pivot+0)->start;
						result += (curr+3)->start ^ (pivot+0)->start;
						result += (curr+0)->start ^ (pivot+1)->start;
						result += (curr+1)->start ^ (pivot+1)->start;
						result += (curr+2)->start ^ (pivot+1)->start;
						result += (curr+3)->start ^ (pivot+1)->start;
						result += (curr+0)->start ^ (pivot+2)->start;
						result += (curr+1)->start ^ (pivot+2)->start;
						result += (curr+2)->start ^ (pivot+2)->start;
						result += (curr+3)->start ^ (pivot+2)->start;
						result += (curr+0)->start ^ (pivot+3)->start;
						result += (curr+1)->start ^ (pivot+3)->start;
						result += (curr+2)->start ^ (pivot+3)->start;
						result += (curr+3)->start ^ (pivot+3)->start;
						result += (curr+0)->start ^ (pivot+4)->start;
						result += (curr+1)->start ^ (pivot+4)->start;
						result += (curr+2)->start ^ (pivot+4)->start;
						result += (curr+3)->start ^ (pivot+4)->start;
						result += (curr+0)->start ^ (pivot+5)->start;
						result += (curr+1)->start ^ (pivot+5)->start;
						result += (curr+2)->start ^ (pivot+5)->start;
						result += (curr+3)->start ^ (pivot+5)->start;
						result += (curr+0)->start ^ (pivot+6)->start;
						result += (curr+1)->start ^ (pivot+6)->start;
						result += (curr+2)->start ^ (pivot+6)->start;
						result += (curr+3)->start ^ (pivot+6)->start;
						result += (curr+0)->start ^ (pivot+7)->start;
						result += (curr+1)->start ^ (pivot+7)->start;
						result += (curr+2)->start ^ (pivot+7)->start;
						result += (curr+3)->start ^ (pivot+7)->start;
						pivot += 8;
					}
					
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 5:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 6:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 7:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 8:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 9:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 10:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 11:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 12:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 13:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 14:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 15:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 16:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 17:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 18:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 19:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 20:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 21:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 22:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 23:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 24:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 25:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 26:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 27:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 28:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						result += (curr+27)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 29:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						result += (curr+27)->start ^ pivot->start;
						result += (curr+28)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 30:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						result += (curr+27)->start ^ pivot->start;
						result += (curr+28)->start ^ pivot->start;
						result += (curr+29)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 31:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						result += (curr+27)->start ^ pivot->start;
						result += (curr+28)->start ^ pivot->start;
						result += (curr+29)->start ^ pivot->start;
						result += (curr+30)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				case 32:
					while (pivot < last)
					{
						result += (curr+0)->start ^ pivot->start;
						result += (curr+1)->start ^ pivot->start;
						result += (curr+2)->start ^ pivot->start;
						result += (curr+3)->start ^ pivot->start;
						result += (curr+4)->start ^ pivot->start;
						result += (curr+5)->start ^ pivot->start;
						result += (curr+6)->start ^ pivot->start;
						result += (curr+7)->start ^ pivot->start;
						result += (curr+8)->start ^ pivot->start;
						result += (curr+9)->start ^ pivot->start;
						result += (curr+10)->start ^ pivot->start;
						result += (curr+11)->start ^ pivot->start;
						result += (curr+12)->start ^ pivot->start;
						result += (curr+13)->start ^ pivot->start;
						result += (curr+14)->start ^ pivot->start;
						result += (curr+15)->start ^ pivot->start;
						result += (curr+16)->start ^ pivot->start;
						result += (curr+17)->start ^ pivot->start;
						result += (curr+18)->start ^ pivot->start;
						result += (curr+19)->start ^ pivot->start;
						result += (curr+20)->start ^ pivot->start;
						result += (curr+21)->start ^ pivot->start;
						result += (curr+22)->start ^ pivot->start;
						result += (curr+23)->start ^ pivot->start;
						result += (curr+24)->start ^ pivot->start;
						result += (curr+25)->start ^ pivot->start;
						result += (curr+26)->start ^ pivot->start;
						result += (curr+27)->start ^ pivot->start;
						result += (curr+28)->start ^ pivot->start;
						result += (curr+29)->start ^ pivot->start;
						result += (curr+30)->start ^ pivot->start;
						result += (curr+31)->start ^ pivot->start;
						pivot++;
					}
					break;
					
				default:
					while (last-pivot >= 32)
					{
						for (auto k = curr; k != lastG; k++)
						{
							result += k->start ^ (pivot+0)->start;
							result += k->start ^ (pivot+1)->start;
							result += k->start ^ (pivot+2)->start;
							result += k->start ^ (pivot+3)->start;
							result += k->start ^ (pivot+4)->start;
							result += k->start ^ (pivot+5)->start;
							result += k->start ^ (pivot+6)->start;
							result += k->start ^ (pivot+7)->start;
							result += k->start ^ (pivot+8)->start;
							result += k->start ^ (pivot+9)->start;
							result += k->start ^ (pivot+10)->start;
							result += k->start ^ (pivot+11)->start;
							result += k->start ^ (pivot+12)->start;
							result += k->start ^ (pivot+13)->start;
							result += k->start ^ (pivot+14)->start;
							result += k->start ^ (pivot+15)->start;
							result += k->start ^ (pivot+16)->start;
							result += k->start ^ (pivot+17)->start;
							result += k->start ^ (pivot+18)->start;
							result += k->start ^ (pivot+19)->start;
							result += k->start ^ (pivot+20)->start;
							result += k->start ^ (pivot+21)->start;
							result += k->start ^ (pivot+22)->start;
							result += k->start ^ (pivot+23)->start;
							result += k->start ^ (pivot+24)->start;
							result += k->start ^ (pivot+25)->start;
							result += k->start ^ (pivot+26)->start;
							result += k->start ^ (pivot+27)->start;
							result += k->start ^ (pivot+28)->start;
							result += k->start ^ (pivot+29)->start;
							result += k->start ^ (pivot+30)->start;
							result += k->start ^ (pivot+31)->start;
						}
						pivot += 32;
					}
					
					if (last-pivot >= 16)
					{
						for (auto k = curr; k != lastG; k++)
						{
							result += k->start ^ (pivot+0)->start;
							result += k->start ^ (pivot+1)->start;
							result += k->start ^ (pivot+2)->start;
							result += k->start ^ (pivot+3)->start;
							result += k->start ^ (pivot+4)->start;
							result += k->start ^ (pivot+5)->start;
							result += k->start ^ (pivot+6)->start;
							result += k->start ^ (pivot+7)->start;
							result += k->start ^ (pivot+8)->start;
							result += k->start ^ (pivot+9)->start;
							result += k->start ^ (pivot+10)->start;
							result += k->start ^ (pivot+11)->start;
							result += k->start ^ (pivot+12)->start;
							result += k->start ^ (pivot+13)->start;
							result += k->start ^ (pivot+14)->start;
							result += k->start ^ (pivot+15)->start;
						}
						pivot += 16;
					}
					
					if (last-pivot >= 8)
					{
						for (auto k = curr; k != lastG; k++)
						{
							result += k->start ^ (pivot+0)->start;
							result += k->start ^ (pivot+1)->start;
							result += k->start ^ (pivot+2)->start;
							result += k->start ^ (pivot+3)->start;
							result += k->start ^ (pivot+4)->start;
							result += k->start ^ (pivot+5)->start;
							result += k->start ^ (pivot+6)->start;
							result += k->start ^ (pivot+7)->start;
						}
						pivot += 8;
					}
					
					if (last-pivot >= 4)
					{
						for (auto k = curr; k != lastG; k++)
						{
							result += k->start ^ (pivot+0)->start;
							result += k->start ^ (pivot+1)->start;
							result += k->start ^ (pivot+2)->start;
							result += k->start ^ (pivot+3)->start;
						}
						pivot += 4;
					}
					
					while (pivot < last)
					{
						auto k = curr;
						while (lastG-k >= 32)
						{
							result += (k+0)->start ^ pivot->start;
							result += (k+1)->start ^ pivot->start;
							result += (k+2)->start ^ pivot->start;
							result += (k+3)->start ^ pivot->start;
							result += (k+4)->start ^ pivot->start;
							result += (k+5)->start ^ pivot->start;
							result += (k+6)->start ^ pivot->start;
							result += (k+7)->start ^ pivot->start;
							result += (k+8)->start ^ pivot->start;
							result += (k+9)->start ^ pivot->start;
							result += (k+10)->start ^ pivot->start;
							result += (k+11)->start ^ pivot->start;
							result += (k+12)->start ^ pivot->start;
							result += (k+13)->start ^ pivot->start;
							result += (k+14)->start ^ pivot->start;
							result += (k+15)->start ^ pivot->start;
							result += (k+16)->start ^ pivot->start;
							result += (k+17)->start ^ pivot->start;
							result += (k+18)->start ^ pivot->start;
							result += (k+19)->start ^ pivot->start;
							result += (k+20)->start ^ pivot->start;
							result += (k+21)->start ^ pivot->start;
							result += (k+22)->start ^ pivot->start;
							result += (k+23)->start ^ pivot->start;
							result += (k+24)->start ^ pivot->start;
							result += (k+25)->start ^ pivot->start;
							result += (k+26)->start ^ pivot->start;
							result += (k+27)->start ^ pivot->start;
							result += (k+28)->start ^ pivot->start;
							result += (k+29)->start ^ pivot->start;
							result += (k+30)->start ^ pivot->start;
							result += (k+31)->start ^ pivot->start;
							k += 32;
						}
						
						if (lastG-k >= 16)
						{
							result += (k+0)->start ^ pivot->start;
							result += (k+1)->start ^ pivot->start;
							result += (k+2)->start ^ pivot->start;
							result += (k+3)->start ^ pivot->start;
							result += (k+4)->start ^ pivot->start;
							result += (k+5)->start ^ pivot->start;
							result += (k+6)->start ^ pivot->start;
							result += (k+7)->start ^ pivot->start;
							result += (k+8)->start ^ pivot->start;
							result += (k+9)->start ^ pivot->start;
							result += (k+10)->start ^ pivot->start;
							result += (k+11)->start ^ pivot->start;
							result += (k+12)->start ^ pivot->start;
							result += (k+13)->start ^ pivot->start;
							result += (k+14)->start ^ pivot->start;
							result += (k+15)->start ^ pivot->start;
							k += 16;
						}
						
						if (lastG-k >= 8)
						{
							result += (k+0)->start ^ pivot->start;
							result += (k+1)->start ^ pivot->start;
							result += (k+2)->start ^ pivot->start;
							result += (k+3)->start ^ pivot->start;
							result += (k+4)->start ^ pivot->start;
							result += (k+5)->start ^ pivot->start;
							result += (k+6)->start ^ pivot->start;
							result += (k+7)->start ^ pivot->start;
							k += 8;
						}
						
						if (lastG-k >= 4)
						{
							result += (k+0)->start ^ pivot->start;
							result += (k+1)->start ^ pivot->start;
							result += (k+2)->start ^ pivot->start;
							result += (k+3)->start ^ pivot->start;
							k += 4;
						}
						
						while (k != lastG)
						{
							result += k->start ^ pivot->start; k++;
						}
						
						pivot++;
					}
					break;
			}
		}
		
		// Sweep the last bucket.
		auto last = BI[cbucket_id].last;
		switch (bufferSize)
		{
			case 1:
				while ((last-pivot >= 32) && (curr->end >= (pivot+31)->start))
				{
					result += curr->start ^ (pivot+0)->start;
					result += curr->start ^ (pivot+1)->start;
					result += curr->start ^ (pivot+2)->start;
					result += curr->start ^ (pivot+3)->start;
					result += curr->start ^ (pivot+4)->start;
					result += curr->start ^ (pivot+5)->start;
					result += curr->start ^ (pivot+6)->start;
					result += curr->start ^ (pivot+7)->start;
					result += curr->start ^ (pivot+8)->start;
					result += curr->start ^ (pivot+9)->start;
					result += curr->start ^ (pivot+10)->start;
					result += curr->start ^ (pivot+11)->start;
					result += curr->start ^ (pivot+12)->start;
					result += curr->start ^ (pivot+13)->start;
					result += curr->start ^ (pivot+14)->start;
					result += curr->start ^ (pivot+15)->start;
					result += curr->start ^ (pivot+16)->start;
					result += curr->start ^ (pivot+17)->start;
					result += curr->start ^ (pivot+18)->start;
					result += curr->start ^ (pivot+19)->start;
					result += curr->start ^ (pivot+20)->start;
					result += curr->start ^ (pivot+21)->start;
					result += curr->start ^ (pivot+22)->start;
					result += curr->start ^ (pivot+23)->start;
					result += curr->start ^ (pivot+24)->start;
					result += curr->start ^ (pivot+25)->start;
					result += curr->start ^ (pivot+26)->start;
					result += curr->start ^ (pivot+27)->start;
					result += curr->start ^ (pivot+28)->start;
					result += curr->start ^ (pivot+29)->start;
					result += curr->start ^ (pivot+30)->start;
					result += curr->start ^ (pivot+31)->start;
					pivot += 32;
				}
				
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += curr->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 2:
				while ((last-pivot >= 16) && (curr->end >= (pivot+15)->start))
				{
					result += (curr+0)->start ^ (pivot+0)->start;
					result += (curr+1)->start ^ (pivot+0)->start;
					result += (curr+0)->start ^ (pivot+1)->start;
					result += (curr+1)->start ^ (pivot+1)->start;
					result += (curr+0)->start ^ (pivot+2)->start;
					result += (curr+1)->start ^ (pivot+2)->start;
					result += (curr+0)->start ^ (pivot+3)->start;
					result += (curr+1)->start ^ (pivot+3)->start;
					result += (curr+0)->start ^ (pivot+4)->start;
					result += (curr+1)->start ^ (pivot+4)->start;
					result += (curr+0)->start ^ (pivot+5)->start;
					result += (curr+1)->start ^ (pivot+5)->start;
					result += (curr+0)->start ^ (pivot+6)->start;
					result += (curr+1)->start ^ (pivot+6)->start;
					result += (curr+0)->start ^ (pivot+7)->start;
					result += (curr+1)->start ^ (pivot+7)->start;
					result += (curr+0)->start ^ (pivot+8)->start;
					result += (curr+1)->start ^ (pivot+8)->start;
					result += (curr+0)->start ^ (pivot+9)->start;
					result += (curr+1)->start ^ (pivot+9)->start;
					result += (curr+0)->start ^ (pivot+10)->start;
					result += (curr+1)->start ^ (pivot+10)->start;
					result += (curr+0)->start ^ (pivot+11)->start;
					result += (curr+1)->start ^ (pivot+11)->start;
					result += (curr+0)->start ^ (pivot+12)->start;
					result += (curr+1)->start ^ (pivot+12)->start;
					result += (curr+0)->start ^ (pivot+13)->start;
					result += (curr+1)->start ^ (pivot+13)->start;
					result += (curr+0)->start ^ (pivot+14)->start;
					result += (curr+1)->start ^ (pivot+14)->start;
					result += (curr+0)->start ^ (pivot+15)->start;
					result += (curr+1)->start ^ (pivot+15)->start;
					pivot += 16;
				}
				
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 3:
				while ((last-pivot >= 8) && (curr->end >= (pivot+7)->start))
				{
					result += (curr+0)->start ^ (pivot+0)->start;
					result += (curr+1)->start ^ (pivot+0)->start;
					result += (curr+2)->start ^ (pivot+0)->start;
					result += (curr+0)->start ^ (pivot+1)->start;
					result += (curr+1)->start ^ (pivot+1)->start;
					result += (curr+2)->start ^ (pivot+1)->start;
					result += (curr+0)->start ^ (pivot+2)->start;
					result += (curr+1)->start ^ (pivot+2)->start;
					result += (curr+2)->start ^ (pivot+2)->start;
					result += (curr+0)->start ^ (pivot+3)->start;
					result += (curr+1)->start ^ (pivot+3)->start;
					result += (curr+2)->start ^ (pivot+3)->start;
					result += (curr+0)->start ^ (pivot+4)->start;
					result += (curr+1)->start ^ (pivot+4)->start;
					result += (curr+2)->start ^ (pivot+4)->start;
					result += (curr+0)->start ^ (pivot+5)->start;
					result += (curr+1)->start ^ (pivot+5)->start;
					result += (curr+2)->start ^ (pivot+5)->start;
					result += (curr+0)->start ^ (pivot+6)->start;
					result += (curr+1)->start ^ (pivot+6)->start;
					result += (curr+2)->start ^ (pivot+6)->start;
					result += (curr+0)->start ^ (pivot+7)->start;
					result += (curr+1)->start ^ (pivot+7)->start;
					result += (curr+2)->start ^ (pivot+7)->start;
					pivot += 8;
				}
				
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 4:
				while ((last-pivot >= 8) && (curr->end >= (pivot+7)->start))
				{
					result += (curr+0)->start ^ (pivot+0)->start;
					result += (curr+1)->start ^ (pivot+0)->start;
					result += (curr+2)->start ^ (pivot+0)->start;
					result += (curr+3)->start ^ (pivot+0)->start;
					result += (curr+0)->start ^ (pivot+1)->start;
					result += (curr+1)->start ^ (pivot+1)->start;
					result += (curr+2)->start ^ (pivot+1)->start;
					result += (curr+3)->start ^ (pivot+1)->start;
					result += (curr+0)->start ^ (pivot+2)->start;
					result += (curr+1)->start ^ (pivot+2)->start;
					result += (curr+2)->start ^ (pivot+2)->start;
					result += (curr+3)->start ^ (pivot+2)->start;
					result += (curr+0)->start ^ (pivot+3)->start;
					result += (curr+1)->start ^ (pivot+3)->start;
					result += (curr+2)->start ^ (pivot+3)->start;
					result += (curr+3)->start ^ (pivot+3)->start;
					result += (curr+0)->start ^ (pivot+4)->start;
					result += (curr+1)->start ^ (pivot+4)->start;
					result += (curr+2)->start ^ (pivot+4)->start;
					result += (curr+3)->start ^ (pivot+4)->start;
					result += (curr+0)->start ^ (pivot+5)->start;
					result += (curr+1)->start ^ (pivot+5)->start;
					result += (curr+2)->start ^ (pivot+5)->start;
					result += (curr+3)->start ^ (pivot+5)->start;
					result += (curr+0)->start ^ (pivot+6)->start;
					result += (curr+1)->start ^ (pivot+6)->start;
					result += (curr+2)->start ^ (pivot+6)->start;
					result += (curr+3)->start ^ (pivot+6)->start;
					result += (curr+0)->start ^ (pivot+7)->start;
					result += (curr+1)->start ^ (pivot+7)->start;
					result += (curr+2)->start ^ (pivot+7)->start;
					result += (curr+3)->start ^ (pivot+7)->start;
					pivot += 8;
				}
				
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 5:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 6:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 7:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 8:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 9:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 10:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 11:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 12:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 13:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 14:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 15:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 16:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 17:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 18:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 19:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 20:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 21:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 22:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 23:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 24:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 25:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 26:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 27:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 28:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					result += (curr+27)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 29:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					result += (curr+27)->start ^ pivot->start;
					result += (curr+28)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 30:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					result += (curr+27)->start ^ pivot->start;
					result += (curr+28)->start ^ pivot->start;
					result += (curr+29)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 31:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					result += (curr+27)->start ^ pivot->start;
					result += (curr+28)->start ^ pivot->start;
					result += (curr+29)->start ^ pivot->start;
					result += (curr+30)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			case 32:
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					result += (curr+0)->start ^ pivot->start;
					result += (curr+1)->start ^ pivot->start;
					result += (curr+2)->start ^ pivot->start;
					result += (curr+3)->start ^ pivot->start;
					result += (curr+4)->start ^ pivot->start;
					result += (curr+5)->start ^ pivot->start;
					result += (curr+6)->start ^ pivot->start;
					result += (curr+7)->start ^ pivot->start;
					result += (curr+8)->start ^ pivot->start;
					result += (curr+9)->start ^ pivot->start;
					result += (curr+10)->start ^ pivot->start;
					result += (curr+11)->start ^ pivot->start;
					result += (curr+12)->start ^ pivot->start;
					result += (curr+13)->start ^ pivot->start;
					result += (curr+14)->start ^ pivot->start;
					result += (curr+15)->start ^ pivot->start;
					result += (curr+16)->start ^ pivot->start;
					result += (curr+17)->start ^ pivot->start;
					result += (curr+18)->start ^ pivot->start;
					result += (curr+19)->start ^ pivot->start;
					result += (curr+20)->start ^ pivot->start;
					result += (curr+21)->start ^ pivot->start;
					result += (curr+22)->start ^ pivot->start;
					result += (curr+23)->start ^ pivot->start;
					result += (curr+24)->start ^ pivot->start;
					result += (curr+25)->start ^ pivot->start;
					result += (curr+26)->start ^ pivot->start;
					result += (curr+27)->start ^ pivot->start;
					result += (curr+28)->start ^ pivot->start;
					result += (curr+29)->start ^ pivot->start;
					result += (curr+30)->start ^ pivot->start;
					result += (curr+31)->start ^ pivot->start;
					pivot++;
				}
				break;
				
			default:
				while ((last-pivot >= 32) && (curr->end >= (pivot+31)->start))
				{
					for (auto k = curr; k != lastG; k++)
					{
						result += k->start ^ (pivot+0)->start;
						result += k->start ^ (pivot+1)->start;
						result += k->start ^ (pivot+2)->start;
						result += k->start ^ (pivot+3)->start;
						result += k->start ^ (pivot+4)->start;
						result += k->start ^ (pivot+5)->start;
						result += k->start ^ (pivot+6)->start;
						result += k->start ^ (pivot+7)->start;
						result += k->start ^ (pivot+8)->start;
						result += k->start ^ (pivot+9)->start;
						result += k->start ^ (pivot+10)->start;
						result += k->start ^ (pivot+11)->start;
						result += k->start ^ (pivot+12)->start;
						result += k->start ^ (pivot+13)->start;
						result += k->start ^ (pivot+14)->start;
						result += k->start ^ (pivot+15)->start;
						result += k->start ^ (pivot+16)->start;
						result += k->start ^ (pivot+17)->start;
						result += k->start ^ (pivot+18)->start;
						result += k->start ^ (pivot+19)->start;
						result += k->start ^ (pivot+20)->start;
						result += k->start ^ (pivot+21)->start;
						result += k->start ^ (pivot+22)->start;
						result += k->start ^ (pivot+23)->start;
						result += k->start ^ (pivot+24)->start;
						result += k->start ^ (pivot+25)->start;
						result += k->start ^ (pivot+26)->start;
						result += k->start ^ (pivot+27)->start;
						result += k->start ^ (pivot+28)->start;
						result += k->start ^ (pivot+29)->start;
						result += k->start ^ (pivot+30)->start;
						result += k->start ^ (pivot+31)->start;
					}
					pivot += 32;
				}
				
				if ((last-pivot >= 16) && (curr->end >= (pivot+15)->start))
				{
					for (auto k = curr; k != lastG; k++)
					{
						result += k->start ^ (pivot+0)->start;
						result += k->start ^ (pivot+1)->start;
						result += k->start ^ (pivot+2)->start;
						result += k->start ^ (pivot+3)->start;
						result += k->start ^ (pivot+4)->start;
						result += k->start ^ (pivot+5)->start;
						result += k->start ^ (pivot+6)->start;
						result += k->start ^ (pivot+7)->start;
						result += k->start ^ (pivot+8)->start;
						result += k->start ^ (pivot+9)->start;
						result += k->start ^ (pivot+10)->start;
						result += k->start ^ (pivot+11)->start;
						result += k->start ^ (pivot+12)->start;
						result += k->start ^ (pivot+13)->start;
						result += k->start ^ (pivot+14)->start;
						result += k->start ^ (pivot+15)->start;
					}
					pivot += 16;
				}
				
				if ((last-pivot >= 8) && (curr->end >= (pivot+7)->start))
				{
					for (auto k = curr; k != lastG; k++)
					{
						result += k->start ^ (pivot+0)->start;
						result += k->start ^ (pivot+1)->start;
						result += k->start ^ (pivot+2)->start;
						result += k->start ^ (pivot+3)->start;
						result += k->start ^ (pivot+4)->start;
						result += k->start ^ (pivot+5)->start;
						result += k->start ^ (pivot+6)->start;
						result += k->start ^ (pivot+7)->start;
					}
					pivot += 8;
				}
				
				if ((last-pivot >= 4) && (curr->end >= (pivot+3)->start))
				{
					for (auto k = curr; k != lastG; k++)
					{
						result += k->start ^ (pivot+0)->start;
						result += k->start ^ (pivot+1)->start;
						result += k->start ^ (pivot+2)->start;
						result += k->start ^ (pivot+3)->start;
					}
					pivot += 4;
				}
				
				while ((pivot < last) && (pivot->start <= curr->end))
				{
					auto k = curr;
					while (lastG-k >= 32)
					{
						result += (k+0)->start ^ pivot->start;
						result += (k+1)->start ^ pivot->start;
						result += (k+2)->start ^ pivot->start;
						result += (k+3)->start ^ pivot->start;
						result += (k+4)->start ^ pivot->start;
						result += (k+5)->start ^ pivot->start;
						result += (k+6)->start ^ pivot->start;
						result += (k+7)->start ^ pivot->start;
						result += (k+8)->start ^ pivot->start;
						result += (k+9)->start ^ pivot->start;
						result += (k+10)->start ^ pivot->start;
						result += (k+11)->start ^ pivot->start;
						result += (k+12)->start ^ pivot->start;
						result += (k+13)->start ^ pivot->start;
						result += (k+14)->start ^ pivot->start;
						result += (k+15)->start ^ pivot->start;
						result += (k+16)->start ^ pivot->start;
						result += (k+17)->start ^ pivot->start;
						result += (k+18)->start ^ pivot->start;
						result += (k+19)->start ^ pivot->start;
						result += (k+20)->start ^ pivot->start;
						result += (k+21)->start ^ pivot->start;
						result += (k+22)->start ^ pivot->start;
						result += (k+23)->start ^ pivot->start;
						result += (k+24)->start ^ pivot->start;
						result += (k+25)->start ^ pivot->start;
						result += (k+26)->start ^ pivot->start;
						result += (k+27)->start ^ pivot->start;
						result += (k+28)->start ^ pivot->start;
						result += (k+29)->start ^ pivot->start;
						result += (k+30)->start ^ pivot->start;
						result += (k+31)->start ^ pivot->start;
						k += 32;
					}
					
					if (lastG-k >= 16)
					{
						result += (k+0)->start ^ pivot->start;
						result += (k+1)->start ^ pivot->start;
						result += (k+2)->start ^ pivot->start;
						result += (k+3)->start ^ pivot->start;
						result += (k+4)->start ^ pivot->start;
						result += (k+5)->start ^ pivot->start;
						result += (k+6)->start ^ pivot->start;
						result += (k+7)->start ^ pivot->start;
						result += (k+8)->start ^ pivot->start;
						result += (k+9)->start ^ pivot->start;
						result += (k+10)->start ^ pivot->start;
						result += (k+11)->start ^ pivot->start;
						result += (k+12)->start ^ pivot->start;
						result += (k+13)->start ^ pivot->start;
						result += (k+14)->start ^ pivot->start;
						result += (k+15)->start ^ pivot->start;
						k += 16;
					}
					
					if (lastG-k >= 8)
					{
						result += (k+0)->start ^ pivot->start;
						result += (k+1)->start ^ pivot->start;
						result += (k+2)->start ^ pivot->start;
						result += (k+3)->start ^ pivot->start;
						result += (k+4)->start ^ pivot->start;
						result += (k+5)->start ^ pivot->start;
						result += (k+6)->start ^ pivot->start;
						result += (k+7)->start ^ pivot->start;
						k += 8;
					}
					
					if (lastG-k >= 4)
					{
						result += (k+0)->start ^ pivot->start;
						result += (k+1)->start ^ pivot->start;
						result += (k+2)->start ^ pivot->start;
						result += (k+3)->start ^ pivot->start;
						k += 4;
					}
					
					while (k != lastG)
					{
						result += k->start ^ pivot->start; k++;
					}
					
					pivot++;
				}
				break;
		}
	}
	
	
	return result;
}



///////////////////////////////////////////////
// Reduced versions for mini-joins breakdown //
///////////////////////////////////////////////
// When R contains a single group in the beginning of the domain, for mini-joins MINIJOIN_REPLICAS_ORIGINALS and MINIJOIN_ORIGINALS_REPLICAS (Rolled)
inline unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Rolled(const Relation& R, const Relation& S, const BucketIndex& BIS)
{
	unsigned long long result = 0;
	auto r = R.begin();
	auto s = S.begin();
	auto lastR = R.end();
	auto lastS = S.end();
	int cbucket_id, pbucket_id;
	Group GR;
	
	
	// Step 1: gather group for R
	while ((r < lastR) && (r->start < s->start))
	{
		GR.emplace_back(r->start, r->end);
		r++;
	}
	
	// Sort current group by end point
	GR.sortByEnd();
	
	
	// Step 2: run internal loop
	result += ForwardScanBased_PlaneSweep_Grouping_Bucketing_InternalLoop_Rolled(GR, s, lastS, BIS, S.minStart);
	
	
	return result;
}


// When R contains a single group in the beginning of the domain, for mini-joins MINIJOIN_REPLICAS_ORIGINALS and MINIJOIN_ORIGINALS_REPLICAS (Unrolled)
inline unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Unrolled(const Relation &R, const Relation &S, const BucketIndex& BIS)
{
	unsigned long long result = 0;
	auto r = R.begin();
	auto s = S.begin();
	auto lastR = R.end();
	auto lastS = S.end();
	int cbucket_id, pbucket_id;
	Group GR;
	
	
	// Step 1: gather group for R
	while (r < lastR)
	{
		GR.emplace_back(r->start, r->end);
		r++;
	}
	
	// Sort current group by end point
	GR.sortByEnd();
	
	// Step 2: run internal loop
	result += ForwardScanBased_PlaneSweep_Grouping_Bucketing_InternalLoop_Unrolled(GR, s, lastS, BIS, S.minStart);
	
	
	return result;
}



//////////////////////////////
// Single-thread processing //
//////////////////////////////
inline unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(Relation &R, Relation &S, const BucketIndex &BIR, const BucketIndex &BIS)
{
	unsigned long long result = 0;
	auto r = R.begin();
	auto s = S.begin();
	auto lastR = R.end();
	auto lastS = S.end();
	Group GR, GS;
	
	
	while ((r < lastR) && (s < lastS))
	{
		if (*r < *s)
		{
			// Step 1: gather group for R.
			while ((r < lastR) && (r->start < s->start))
			{
				GR.emplace_back(r->start, r->end);
				r++;
			}
			
			// Sort current group by end point.
			GR.sortByEnd();


			// Step 2: run internal loop.
			result += ForwardScanBased_PlaneSweep_Grouping_Bucketing_InternalLoop_Rolled(GR, s, lastS, BIS, S.minStart);
			
			
			// Step 3: empty current group.
			GR.clear();
		}
		else
		{
			// Step 1: gather group for S.
			while ((s < lastS) && (r->start >= s->start))
			{
				GS.emplace_back(s->start, s->end);
				s++;
			}
			
			// Sort current group by end point.
			GS.sortByEnd();
			
			
			// Step 2: run internal loop.
			result += ForwardScanBased_PlaneSweep_Grouping_Bucketing_InternalLoop_Rolled(GS, r, lastR, BIR, R.minStart);

			
			// Step 3: empty current group.
			GS.clear();
		}
	}
	
	
	return result;
}


inline unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(Relation &R, Relation &S, const BucketIndex &BIR, const BucketIndex &BIS)
{
	unsigned long long result = 0;
	auto r = R.begin();
	auto s = S.begin();
	auto lastR = R.end();
	auto lastS = S.end();
	Group GR, GS;
	
	
	while ((r < lastR) && (s < lastS))
	{
		if (*r < *s)
		{
			// Step 1: gather group for R.
			while ((r < lastR) && (r->start < s->start))
			{
				GR.emplace_back(r->start, r->end);
				r++;
			}
			
			// Sort current group by end point.
			GR.sortByEnd();
			
			// Step 2: run internal loop.
			result += ForwardScanBased_PlaneSweep_Grouping_Bucketing_InternalLoop_Unrolled(GR, s, lastS, BIS, S.minStart);


			// Step 3: empty current group.
			GR.clear();
		}
		else
		{
			// Step 1: gather group for S.
			while ((s < lastS) && (r->start >= s->start))
			{
				GS.emplace_back(s->start, s->end);
				s++;
			}
			
			// Sort current group by end point.
			GS.sortByEnd();
			
			
			// Step 2: run internal loop.
			result += ForwardScanBased_PlaneSweep_Grouping_Bucketing_InternalLoop_Unrolled(GS, r, lastR, BIR, R.minStart);

			
			// Step 3: empty current group.
			GS.clear();
		}
	}
	
	
	return result;
}


unsigned long long ForwardScanBased_PlaneSweep_Grouping_Bucketing(Relation &R, Relation &S, const BucketIndex &BIR, const BucketIndex &BIS, bool runUnrolled)
{
	if (runUnrolled)
		return ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(R, S, BIR, BIS);
	else
		return ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(R, S, BIR, BIS);
}



////////////////////////////////////
// Hash-based parallel processing //
////////////////////////////////////
inline unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads)
{
	unsigned long long result = 0;


	#pragma omp parallel for num_threads(runNumThreads) collapse(2) reduction(+ : result)
	for (int i = 0; i < runNumPartitionsPerRelation; i++)
	{
		for (int j = 0; j < runNumPartitionsPerRelation; j++)
		{
			result += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(pR[i], pS[j], pBIR[i], pBIS[j]);
		}
	}


	return result;
}


inline unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads)
{
	unsigned long long result = 0;


	#pragma omp parallel for num_threads(runNumThreads) collapse(2) reduction(+ : result)
	for (int i = 0; i < runNumPartitionsPerRelation; i++)
	{
		for (int j = 0; j < runNumPartitionsPerRelation; j++)
		{
			result += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(pR[i], pS[j], pBIR[i], pBIS[j]);
		}
	}
	
	
	return result;
}


unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing(Relation *pR, Relation *pS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads, bool runUnrolled)
{
	if (runUnrolled)
		return ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(pR, pS, pBIR, pBIS, runNumPartitionsPerRelation, runNumThreads);
	else
		return ParallelHashBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(pR, pS, pBIR, pBIS, runNumPartitionsPerRelation, runNumThreads);
}



//////////////////////////////////////
// Domain-based parallel processing //
//////////////////////////////////////
// For the atomic/uniform or the atomic/adaptive setup (Rolled).
inline unsigned long long ParallelDomainBased_Atomic_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads)
{
	unsigned long long result = 0;
	
	
	#pragma omp parallel for num_threads(runNumThreads) reduction(+: result)
	for (int i = 0; i < runNumPartitionsPerRelation; i++)
	{
		unsigned long long res = 0;
		
		res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(pR[i], pS[i], pBIR[i], pBIS[i]);
		
		// Break down to avoid computing duplicate results in the first place.
		if (i != 0)
		{
			res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Rolled(prR[i], pS[i], pBIS[i]);
			res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Rolled(prS[i], pR[i], pBIR[i]);	// ALERT: Input order inverted.
		}
		result += res;
	}
	
	
	return result;
}


// For the atomic/uniform or the atomic/adaptive setup (Unolled).
inline unsigned long long ParallelDomainBased_Atomic_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads)
{
	unsigned long long result = 0;
	
	
	#pragma omp parallel for num_threads(runNumThreads) reduction(+: result)
	for (int i = 0; i < runNumPartitionsPerRelation; i++)
	{
		unsigned long long res = 0;
		
		res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(pR[i], pS[i], pBIR[i], pBIS[i]);
		
		// Break down to avoid computing duplicate results in the first place.
		if (i != 0)
		{
			res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Unrolled(prR[i], pS[i], pBIS[i]);
			res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Unrolled(prS[i], pR[i], pBIR[i]);	// ALERT: Input order inverted.
		}
		result += res;
	}
	
	
	return result;
}


// For the mj+atomic/uniform or the mj+atomic/adaptive setup (Rolled)
inline unsigned long long ParallelDomainBased_MiniJoinsAtomic_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads)
{
	unsigned long long result = 0;
	
	
	#pragma omp parallel for num_threads(runNumThreads) reduction(+: result)
	for (int i = 0; i < runNumPartitionsPerRelation; i++)
	{
		unsigned long long res = 0;
		
		// Mini-join: MINIJOIN_ORIGINALS_ORIGINALS.
		res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(pR[i], pS[i], pBIR[i], pBIS[i]);
		if (i != 0)
		{
			// Mini-joins: MINIJOIN_REPLICAS_ORIGINALS and MINIJOIN_ORIGINALS_REPLICAS.
			res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Rolled(prR[i], pS[i], pBIS[i]);
			res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Rolled(prS[i], pR[i], pBIR[i]);	// ALERT: Input order inverted.
			
			// Mini-joins: MINIJOIN_FREPLICAS_ORIGINALS and MINIJOIN_ORIGINALS_FREPLICAS.
			res += NestedLoops_Rolled(prfR[i], pS[i]);
			res += NestedLoops_Rolled(prfS[i], pR[i]);	// ALERT: Input order inverted.
		}
		result += res;
	}
	
	
	return result;
}


// For the mj+atomic/uniform or the mj+atomic/adaptive setup (Unrolled)
inline unsigned long long ParallelDomainBased_MiniJoinsAtomic_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads)
{
	unsigned long long result = 0;
	
	
	#pragma omp parallel for num_threads(runNumThreads) reduction(+: result)
	for (int i = 0; i < runNumPartitionsPerRelation; i++)
	{
		unsigned long long res = 0;
		
		// Mini-join: MINIJOIN_ORIGINALS_ORIGINALS.
		res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(pR[i], pS[i], pBIR[i], pBIS[i]);
		if (i != 0)
		{
			// Mini-joins: MINIJOIN_REPLICAS_ORIGINALS and MINIJOIN_ORIGINALS_REPLICAS.
			res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Unrolled(prR[i], pS[i], pBIS[i]);
			res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Unrolled(prS[i], pR[i], pBIR[i]);	// ALERT: Input order inverted.
			
			// Mini-joins: MINIJOIN_FREPLICAS_ORIGINALS and MINIJOIN_ORIGINALS_FREPLICAS.
			res += NestedLoops_Unrolled(prfR[i], pS[i]);
			res += NestedLoops_Unrolled(prfS[i], pR[i]);	// ALERT: Input order inverted.
		}
		result += res;
	}
	
	
	return result;
}


// For the mj+greedy/uniform or the mj+greedy/adaptive setup (Rolled)
inline unsigned long long ParallelDomainBased_MiniJoinsGreedy_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads)
{
	int runNumPartitions = runNumThreads;
	unsigned long long result = 0;
	GreedyScheduler GS;
	
	
	GS.schedule(pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads);
	
	#pragma omp parallel for num_threads(runNumThreads) reduction(+ : result)
	for (int t = 0; t < runNumThreads; t++)
	{
		unsigned long long res = 0;
		
		for (const std::pair<int, int>& mj : GS.TBH[t].mjoins)
		{
			switch (mj.second)
			{
				case MINIJOIN_ORIGINALS_ORIGINALS:
					res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(pR[mj.first], pS[mj.first], pBIR[mj.first], pBIS[mj.first]);
					break;
				case MINIJOIN_REPLICAS_ORIGINALS:
					res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Rolled(prR[mj.first], pS[mj.first], pBIS[mj.first]);
					break;
				case MINIJOIN_ORIGINALS_REPLICAS:
					res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Rolled(prS[mj.first], pR[mj.first], pBIR[mj.first]);	// ALERT: Input order inverted.
					break;
				case MINIJOIN_FREPLICAS_ORIGINALS:
					res += NestedLoops_Rolled(prfR[mj.first], pS[mj.first]);
					break;
				case MINIJOIN_ORIGINALS_FREPLICAS:
					res += NestedLoops_Rolled(prfS[mj.first], pR[mj.first]);	// ALERT: Input order inverted.
					break;
			}
		}
		result += res;
	}
	
	
	return result;
}


// For the mj+greedy/uniform or the mj+greedy/adaptive setup (Unrolled)
inline unsigned long long ParallelDomainBased_MiniJoinsGreedy_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads)
{
	int runNumPartitions = runNumThreads;
	unsigned long long result = 0;
	GreedyScheduler GS;
	
	
	GS.schedule(pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads);
	
	#pragma omp parallel for num_threads(runNumThreads) reduction(+ : result)
	for (int t = 0; t < runNumThreads; t++)
	{
		unsigned long long res = 0;
		
		for (const std::pair<int, int>& mj : GS.TBH[t].mjoins)
		{
			switch (mj.second)
			{
				case MINIJOIN_ORIGINALS_ORIGINALS:
					res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(pR[mj.first], pS[mj.first], pBIR[mj.first], pBIS[mj.first]);
					break;
				case MINIJOIN_REPLICAS_ORIGINALS:
					res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Unrolled(prR[mj.first], pS[mj.first], pBIS[mj.first]);
					break;
				case MINIJOIN_ORIGINALS_REPLICAS:
					res += ForwardScanBased_PlaneSweep_Grouping_Bucketing_Reduced_Unrolled(prS[mj.first], pR[mj.first], pBIR[mj.first]);	// ALERT: Input order inverted.
					break;
				case MINIJOIN_FREPLICAS_ORIGINALS:
					res += NestedLoops_Unrolled(prfR[mj.first], pS[mj.first]);
					break;
				case MINIJOIN_ORIGINALS_FREPLICAS:
					res += NestedLoops_Unrolled(prfS[mj.first], pR[mj.first]);	// ALERT: Input order inverted.
					break;
			}
		}
		result += res;
	}
	
	
	return result;
}


unsigned long long ParallelDomainBased_ForwardScanBased_PlaneSweep_Grouping_Bucketing(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumPartitionsPerRelation, int runNumThreads, bool runUnrolled, bool runGreedyScheduling, bool runMiniJoinsBreakDown, bool runAdaptivePartitioning)
{
	if (runUnrolled)
	{
		if (runMiniJoinsBreakDown)
		{
			if (runGreedyScheduling)
			{
				// mj+greedy setups
				return ParallelDomainBased_MiniJoinsGreedy_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(pR, pS, prR, prS, prfR, prfS, pBIR, pBIS, runNumPartitionsPerRelation, runNumThreads);
			}
			else
			{
				// mj+atomic setups
				return ParallelDomainBased_MiniJoinsAtomic_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(pR, pS, prR, prS, prfR, prfS, pBIR, pBIS, runNumPartitionsPerRelation, runNumThreads);
			}
		}
		else
		{
			// atomic setups
			return ParallelDomainBased_Atomic_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Unrolled(pR, pS, prR, prS, pBIR, pBIS, runNumPartitionsPerRelation, runNumThreads);
		}
	}
	else
	{
		if (runMiniJoinsBreakDown)
		{
			if (runGreedyScheduling)
			{
				// mj+greedy setups
				return ParallelDomainBased_MiniJoinsGreedy_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(pR, pS, prR, prS, prfR, prfS, pBIR, pBIS, runNumPartitionsPerRelation, runNumThreads);
			}
			else
			{
				// mj+atomic setups
				return ParallelDomainBased_MiniJoinsAtomic_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(pR, pS, prR, prS, prfR, prfS, pBIR, pBIS, runNumPartitionsPerRelation, runNumThreads);
			}
		}
		else
		{
			// atomic setups
			return ParallelDomainBased_Atomic_ForwardScanBased_PlaneSweep_Grouping_Bucketing_Rolled(pR, pS, prR, prS, pBIR, pBIS, runNumPartitionsPerRelation, runNumThreads);
		}
	}
}
