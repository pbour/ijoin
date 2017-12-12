#ifndef _RELATION_H_
#define _RELATION_H_

#include "../def.h"



class Record
{
public:
	Timestamp start;
	Timestamp end;

	Record();
	Record(Timestamp start, Timestamp end);
	bool operator < (const Record& rhs) const;
	bool operator >= (const Record& rhs) const;
	void print() const;
	void print(char c) const;
	~Record();	
};



class Relation : public vector<Record>
{
public:
	size_t numRecords;
	Timestamp minStart, maxStart, minEnd, maxEnd;
	Timestamp longestRecord;

	Relation();
	void load(const char *filename);
	void load(const Relation& I, size_t from = 0, size_t by = 1);
	void sortByStart();
	void sortByEnd();
	void print(char c);
	~Relation();
};
typedef Relation::const_iterator RelationIterator;

typedef Relation Group;
typedef Group::const_iterator GroupIterator;
#endif //_RELATION_H_
