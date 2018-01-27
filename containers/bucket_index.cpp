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


#include "bucket_index.h"



Bucket::Bucket()
{
}


Bucket::Bucket(RelationIterator i)
{
	this->last = i;
}


Bucket::~Bucket()
{
}



BucketIndex::BucketIndex()
{
}


void BucketIndex::build(const Relation &R, int numBuckets)
{
	int cbucket_id = 0, btmp;
	RelationIterator i = R.begin(), lastI = R.end();
	auto ms = R.maxStart;

	
	if (R.minStart == R.maxStart)
		ms += 1;
	
	this->numBuckets = numBuckets;
	this->bucket_range = (Timestamp)ceil((double)(ms-R.minStart)/this->numBuckets);
	this->reserve(this->numBuckets);
	for (int i = 0; i < this->numBuckets; i++)
		this->push_back(Bucket(lastI));

	while (i != lastI)
	{
		btmp = ceil((double)(i->start-R.minStart)/this->bucket_range);
		if (btmp >= this->numBuckets)
			btmp = this->numBuckets-1;
		
		if (cbucket_id != btmp)
		{
			(*this)[cbucket_id].last = i;
			cbucket_id++;
		}
		else
		{
			++i;
		}
	}
	(*this)[this->numBuckets-1].last = lastI;
	for (int i = this->numBuckets-2; i >= 0; i--)
	{
		if ((*this)[i].last == lastI)
			(*this)[i].last = (*this)[i+1].last;
	}
}


void BucketIndex::print(char c)
{
	cout << "Bucket Index" << endl;
	cout << "============" << endl;
	cout << "bucket range = " << this->bucket_range << endl;
	for (int bid = 0; bid < this->numBuckets-1; bid++)
		cout << "bucket " << bid << ": " << c << "[" << (*this)[bid].last->start << ", " << (*this)[bid].last->end << "]" << endl;
	cout << "bucket " << (this->numBuckets-1) << ": NULL" << endl;
}


BucketIndex::~BucketIndex()
{
}
