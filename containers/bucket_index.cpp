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
