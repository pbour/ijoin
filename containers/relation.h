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
