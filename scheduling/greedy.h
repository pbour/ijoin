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
