#ifndef _BUCKET_INDEX_H_
#define _BUCKET_INDEX_H_

#include "../def.h"
#include "relation.h"



class Bucket
{
public:
	RelationIterator last;

	Bucket();
    Bucket(RelationIterator i);
	~Bucket();
};



class BucketIndex : public vector<Bucket>
{
public:
	int numBuckets;
	Timestamp bucket_range;
	
    BucketIndex();
	void build(const Relation &R, int numBuckets);
	void print(char c);
	~BucketIndex();
};

typedef vector<Bucket>::const_iterator BucketIndexIterator;
#endif //_BUCKET_INDEX_H_
