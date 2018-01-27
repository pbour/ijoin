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


#include "relation.h"



bool CompareByEnd(const Record& lhs, const Record& rhs)
{
	return (lhs.end < rhs.end);
}



Record::Record()
{
}


//Record::Record(RecordId id, Timestamp start, Timestamp end)
Record::Record(Timestamp start, Timestamp end)
{
	this->start = start;
	this->end = end;
}


bool Record::operator < (const Record& rhs) const
{
	return this->start < rhs.start;
}

bool Record::operator >= (const Record& rhs) const
{
	return !((*this) < rhs);
}


void Record::print(char c) const
{
	cout << c << "[" << this->start << ".." << this->end << "]" << endl;
}

   
Record::~Record()
{
}



Relation::Relation()
{
	this->minStart = std::numeric_limits<Timestamp>::max();
	this->maxStart = std::numeric_limits<Timestamp>::min();
	this->minEnd   = std::numeric_limits<Timestamp>::max();
	this->maxEnd   = std::numeric_limits<Timestamp>::min();
	this->longestRecord = std::numeric_limits<Timestamp>::min();
}


void Relation::load(const char *filename)
{
	Timestamp start, end;
	ifstream inp(filename);

	
	if (!inp)
	{
		cerr << "error - cannot open file " << filename << endl;
		exit(1);
	}

	while (inp >> start >> end)
	{
		this->emplace_back(start, end);

		this->minStart = std::min(this->minStart, start);
		this->maxStart = std::max(this->maxStart, start);
		this->minEnd   = std::min(this->minEnd  , end);
		this->maxEnd   = std::max(this->maxEnd  , end);
		this->longestRecord = std::max(this->longestRecord, end-start+1);
	}
	inp.close();
	
	this->numRecords = this->size();
}


void Relation::load(const Relation& I, size_t from, size_t by)
{
	size_t tupleCount = (I.size() + by - 1 - from) / by;


	for (size_t i = from; i < I.size(); i += by)
	{
		this->emplace_back(I[i].start, I[i].end);

		this->minStart = std::min(this->minStart, I[i].start);
		this->maxStart = std::max(this->maxStart, I[i].start);
		this->minEnd   = std::min(this->minEnd  , I[i].end);
		this->maxEnd   = std::max(this->maxEnd  , I[i].end);
		this->longestRecord = std::max(this->longestRecord, I[i].end-I[i].start+1);
	}
	sort(this->begin(), this->end());
}


void Relation::sortByStart()
{
	sort(this->begin(), this->end());
}


void Relation::sortByEnd()
{
	sort(this->begin(), this->end(), CompareByEnd);
}


void Relation::print(char c)
{
	for (const Record& rec : (*this))
		rec.print(c);
}


Relation::~Relation()
{
}
