#include "../def.h"
#include "../containers/relation.h"
#include "../scheduling/greedy.h"



unsigned long long NestedLoops_Rolled(const Relation &R, const Relation &S);
unsigned long long NestedLoops_Unrolled(const Relation &R, const Relation &S);



////////////////////
// Internal loops //
////////////////////
inline unsigned long long ForwardScanBased_PlaneSweep_InternalLoop_Rolled(RelationIterator rec, RelationIterator firstFS, RelationIterator lastFS)
{
    unsigned long long result = 0;
    auto pivot = firstFS;


    while ((pivot < lastFS) && (rec->end >= pivot->start))
    {
        result += rec->start ^ pivot->start;
        pivot++;
    }
    
    
    return result;
}


inline unsigned long long ForwardScanBased_PlaneSweep_Rolled(Relation &R, Relation &S)
{
    unsigned long long result = 0;
    auto r = R.begin();
    auto s = S.begin();
    auto lastR = R.end();
    auto lastS = S.end();
    
    
    while ((r < lastR) && (s < lastS))
    {
        if (*r < *s)
        {
            // Run internal loop.
            result += ForwardScanBased_PlaneSweep_InternalLoop_Rolled(r, s, lastS);
            r++;
        }
        else
        {
            // Run internal loop.
            result += ForwardScanBased_PlaneSweep_InternalLoop_Rolled(s, r, lastR);
            s++;
        }
    }
    
    
    return result;
}


inline unsigned long long ForwardScanBased_PlaneSweep_InternalLoop_Unrolled(RelationIterator rec, RelationIterator firstFS, RelationIterator lastFS)
{
    unsigned long long result = 0;
    auto pivot = firstFS;


    while ((lastFS-pivot >= 32) && (rec->end >= (pivot+31)->start))
    {
        result += rec->start ^ (pivot+0)->start;
        result += rec->start ^ (pivot+1)->start;
        result += rec->start ^ (pivot+2)->start;
        result += rec->start ^ (pivot+3)->start;
        result += rec->start ^ (pivot+4)->start;
        result += rec->start ^ (pivot+5)->start;
        result += rec->start ^ (pivot+6)->start;
        result += rec->start ^ (pivot+7)->start;
        result += rec->start ^ (pivot+8)->start;
        result += rec->start ^ (pivot+9)->start;
        result += rec->start ^ (pivot+10)->start;
        result += rec->start ^ (pivot+11)->start;
        result += rec->start ^ (pivot+12)->start;
        result += rec->start ^ (pivot+13)->start;
        result += rec->start ^ (pivot+14)->start;
        result += rec->start ^ (pivot+15)->start;
        result += rec->start ^ (pivot+16)->start;
        result += rec->start ^ (pivot+17)->start;
        result += rec->start ^ (pivot+18)->start;
        result += rec->start ^ (pivot+19)->start;
        result += rec->start ^ (pivot+20)->start;
        result += rec->start ^ (pivot+21)->start;
        result += rec->start ^ (pivot+22)->start;
        result += rec->start ^ (pivot+23)->start;
        result += rec->start ^ (pivot+24)->start;
        result += rec->start ^ (pivot+25)->start;
        result += rec->start ^ (pivot+26)->start;
        result += rec->start ^ (pivot+27)->start;
        result += rec->start ^ (pivot+28)->start;
        result += rec->start ^ (pivot+29)->start;
        result += rec->start ^ (pivot+30)->start;
        result += rec->start ^ (pivot+31)->start;
        pivot += 32;
    }
    
    if ((lastFS-pivot >= 16) && (rec->end >= (pivot+15)->start))
    {
        result += rec->start ^ (pivot+0)->start;
        result += rec->start ^ (pivot+1)->start;
        result += rec->start ^ (pivot+2)->start;
        result += rec->start ^ (pivot+3)->start;
        result += rec->start ^ (pivot+4)->start;
        result += rec->start ^ (pivot+5)->start;
        result += rec->start ^ (pivot+6)->start;
        result += rec->start ^ (pivot+7)->start;
        result += rec->start ^ (pivot+8)->start;
        result += rec->start ^ (pivot+9)->start;
        result += rec->start ^ (pivot+10)->start;
        result += rec->start ^ (pivot+11)->start;
        result += rec->start ^ (pivot+12)->start;
        result += rec->start ^ (pivot+13)->start;
        result += rec->start ^ (pivot+14)->start;
        result += rec->start ^ (pivot+15)->start;
        pivot += 16;
    }
    
    if ((lastFS-pivot >= 8) && (rec->end >= (pivot+7)->start))
    {
        result += rec->start ^ (pivot+0)->start;
        result += rec->start ^ (pivot+1)->start;
        result += rec->start ^ (pivot+2)->start;
        result += rec->start ^ (pivot+3)->start;
        result += rec->start ^ (pivot+4)->start;
        result += rec->start ^ (pivot+5)->start;
        result += rec->start ^ (pivot+6)->start;
        result += rec->start ^ (pivot+7)->start;
        pivot += 8;
    }
    
    if ((lastFS-pivot >= 4) && (rec->end >= (pivot+3)->start))
    {
        result += rec->start ^ (pivot+0)->start;
        result += rec->start ^ (pivot+1)->start;
        result += rec->start ^ (pivot+2)->start;
        result += rec->start ^ (pivot+3)->start;
        pivot += 4;
    }
    
    while ((pivot < lastFS) && (rec->end >= pivot->start))
    {
        result += rec->start ^ pivot->start; pivot++;
    }

    
    return result;
}




/////////////////////////////////
// Single-threaded processing //
/////////////////////////////////
inline unsigned long long ForwardScanBased_PlaneSweep_Unrolled(Relation &R, Relation &S)
{
    unsigned long long result = 0;
    auto r = R.begin();
    auto s = S.begin();
    auto lastR = R.end();
    auto lastS = S.end();
    
    
    while ((r < lastR) && (s < lastS))
    {
        if (*r < *s)
        {
            // Run internal loop.
            result += ForwardScanBased_PlaneSweep_InternalLoop_Unrolled(r, s, lastS);
            r++;
        }
        else
        {
            // Run internal loop.
            result += ForwardScanBased_PlaneSweep_InternalLoop_Unrolled(s, r, lastR);
            s++;
        }
    }
    
    
    return result;
}


unsigned long long ForwardScanBased_PlaneSweep(Relation &R, Relation &S, bool runUnrolled)
{
    if (runUnrolled)
        return ForwardScanBased_PlaneSweep_Unrolled(R, S);
    else
        return ForwardScanBased_PlaneSweep_Rolled(R, S);
}



////////////////////////////////////
// Hash-based parallel processing //
////////////////////////////////////
inline unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Rolled(Relation *pR, Relation *pS, int runNumPartitionsPerRelation, int runNumThreads)
{
    unsigned long long result = 0;
    
    
    #pragma omp parallel for num_threads(runNumThreads) collapse(2) reduction(+ : result)
    for (int i = 0; i < runNumPartitionsPerRelation; i++)
    {
        for (int j = 0; j < runNumPartitionsPerRelation; j++)
        {
            result += ForwardScanBased_PlaneSweep_Rolled(pR[i], pS[j]);
        }
    }
    
    
    return result;
}


inline unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Unrolled(Relation *pR, Relation *pS, int runNumPartitionsPerRelation, int runNumThreads)
{
    unsigned long long result = 0;
    
    
    #pragma omp parallel for num_threads(runNumThreads) collapse(2) reduction(+ : result)
    for (int i = 0; i < runNumPartitionsPerRelation; i++)
    {
        for (int j = 0; j < runNumPartitionsPerRelation; j++)
        {
            result += ForwardScanBased_PlaneSweep_Unrolled(pR[i], pS[j]);
        }
    }
    
    
    return result;
}


unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep(Relation *pR, Relation *pS, int runNumPartitionsPerRelation, int runNumThreads, bool runUnrolled)
{
    if (runUnrolled)
        return ParallelHashBased_ForwardScanBased_PlaneSweep_Unrolled(pR, pS, runNumPartitionsPerRelation, runNumThreads);
    else
        return ParallelHashBased_ForwardScanBased_PlaneSweep_Rolled(pR, pS, runNumPartitionsPerRelation, runNumThreads);
}



//////////////////////////////////////
// Domain-based parallel processing //
//////////////////////////////////////
// For the atomic/uniform or the atomic/adaptive setup (Rolled).
inline unsigned long long ParallelDomainBased_Atomic_ForwardScanBased_PlaneSweep_Rolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, int runNumPartitionsPerRelation, int runNumThreads)
{
    unsigned long long result = 0;
    
    
    #pragma omp parallel for num_threads(runNumThreads) reduction(+: result)
    for (int i = 0; i < runNumPartitionsPerRelation; i++)
    {
        unsigned long long res = 0;
        
        res += ForwardScanBased_PlaneSweep_Rolled(pR[i], pS[i]);
        
        // Break down to avoid computing duplicate results in the first place.
        if (i != 0)
        {
            res += ForwardScanBased_PlaneSweep_Rolled(prR[i], pS[i]);
            res += ForwardScanBased_PlaneSweep_Rolled(pR[i] , prS[i]);
        }
        result += res;
    }
    
    
    return result;
}


// For the atomic/uniform or the atomic/adaptive setup (Unolled).
inline unsigned long long ParallelDomainBased_Atomic_ForwardScanBased_PlaneSweep_Unrolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, int runNumPartitionsPerRelation, int runNumThreads)
{
    unsigned long long result = 0;
    
    
    #pragma omp parallel for num_threads(runNumThreads) reduction(+: result)
    for (int i = 0; i < runNumPartitionsPerRelation; i++)
    {
        unsigned long long res = 0;
        
        res += ForwardScanBased_PlaneSweep_Unrolled(pR[i], pS[i]);
        
        // Break down to avoid computing duplicate results in the first place.
        if (i != 0)
        {
            res += ForwardScanBased_PlaneSweep_Unrolled(prR[i], pS[i]);
            res += ForwardScanBased_PlaneSweep_Unrolled(pR[i] , prS[i]);    // ALERT: Input order inverted.
        }
        result += res;
    }
    
    
    return result;
}


// For the mj+atomic/uniform or the mj+atomic/adaptive setup (Rolled)
inline unsigned long long ParallelDomainBased_MiniJoinsAtomic_ForwardScanBased_PlaneSweep_Rolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads)
{
    unsigned long long result = 0;
    
    
    #pragma omp parallel for num_threads(runNumThreads) reduction(+: result)
    for (int i = 0; i < runNumPartitionsPerRelation; i++)
    {
        unsigned long long res = 0;
        
        // Mini-join: MINIJOIN_ORIGINALS_ORIGINALS.
        res += ForwardScanBased_PlaneSweep_Rolled(pR[i], pS[i]);
        if (i != 0)
        {
            // Mini-joins: MINIJOIN_REPLICAS_ORIGINALS and MINIJOIN_ORIGINALS_REPLICAS.
            res += ForwardScanBased_PlaneSweep_Rolled(prR[i], pS[i]);
            res += ForwardScanBased_PlaneSweep_Rolled(pR[i] , prS[i]);
            
            // Mini-joins: MINIJOIN_FREPLICAS_ORIGINALS and MINIJOIN_ORIGINALS_FREPLICAS.
            res += NestedLoops_Rolled(prfR[i], pS[i]);
            res += NestedLoops_Rolled(prfS[i], pR[i]);    // ALERT: Input order inverted.
        }
        result += res;
    }
    
    
    return result;
}


// For the mj+atomic/uniform or the mj+atomic/adaptive setup (Unrolled)
inline unsigned long long ParallelDomainBased_MiniJoinsAtomic_ForwardScanBased_PlaneSweep_Unrolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads)
{
    unsigned long long result = 0;
    
    
    #pragma omp parallel for num_threads(runNumThreads) reduction(+: result)
    for (int i = 0; i < runNumPartitionsPerRelation; i++)
    {
        unsigned long long res = 0;
        
        // Mini-join: MINIJOIN_ORIGINALS_ORIGINALS.
        res += ForwardScanBased_PlaneSweep_Unrolled(pR[i], pS[i]);
        if (i != 0)
        {
            // Mini-joins: MINIJOIN_REPLICAS_ORIGINALS and MINIJOIN_ORIGINALS_REPLICAS.
            res += ForwardScanBased_PlaneSweep_Unrolled(prR[i], pS[i]);
            res += ForwardScanBased_PlaneSweep_Unrolled(pR[i] , prS[i]);
            
            // Mini-joins: MINIJOIN_FREPLICAS_ORIGINALS and MINIJOIN_ORIGINALS_FREPLICAS.
            res += NestedLoops_Unrolled(prfR[i], pS[i]);
            res += NestedLoops_Unrolled(prfS[i], pR[i]);    // ALERT: Input order inverted.
        }
        result += res;
    }
    
    
    return result;
}


// For the mj+greedy/uniform or the mj+greedy/adaptive setup (Rolled)
inline unsigned long long ParallelDomainBased_MiniJoinsGreedy_ForwardScanBased_PlaneSweep_Rolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads)
{
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
                    res += ForwardScanBased_PlaneSweep_Rolled(pR[mj.first], pS[mj.first]);
                    break;
                case MINIJOIN_REPLICAS_ORIGINALS:
                    res += ForwardScanBased_PlaneSweep_Rolled(prR[mj.first], pS[mj.first]);
                    break;
                case MINIJOIN_ORIGINALS_REPLICAS:
                    res += ForwardScanBased_PlaneSweep_Rolled(pR[mj.first], prS[mj.first]);
                    break;
                case MINIJOIN_FREPLICAS_ORIGINALS:
                    res += NestedLoops_Rolled(prfR[mj.first], pS[mj.first]);
                    break;
                case MINIJOIN_ORIGINALS_FREPLICAS:
                    res += NestedLoops_Rolled(prfS[mj.first], pR[mj.first]);    // ALERT: Input order inverted.
                    break;
            }
        }
        result += res;
    }
    
    
    return result;
}


inline unsigned long long ParallelDomainBased_MiniJoinsGreedy_ForwardScanBased_PlaneSweep_Unrolled(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads)
{
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
                    res += ForwardScanBased_PlaneSweep_Unrolled(pR[mj.first], pS[mj.first]);
                    break;
                case MINIJOIN_REPLICAS_ORIGINALS:
                    res += ForwardScanBased_PlaneSweep_Unrolled(prR[mj.first], pS[mj.first]);
                    break;
                case MINIJOIN_ORIGINALS_REPLICAS:
                    res += ForwardScanBased_PlaneSweep_Unrolled(pR[mj.first], prS[mj.first]);
                    break;
                case MINIJOIN_FREPLICAS_ORIGINALS:
                    res += NestedLoops_Unrolled(prfR[mj.first], pS[mj.first]);
                    break;
                case MINIJOIN_ORIGINALS_FREPLICAS:
                    res += NestedLoops_Unrolled(prfS[mj.first], pR[mj.first]);    // ALERT: Input order inverted.
                    break;
            }
        }
        result += res;
    }
    
    
    return result;
}


unsigned long long ParallelDomainBased_ForwardScanBased_PlaneSweep(Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads, bool runUnrolled, bool runGreedyScheduling, bool runMiniJoinsBreakDown, bool runAdaptivePartitioning)
{
    if (runUnrolled)
    {
        if (runMiniJoinsBreakDown)
        {
            if (runGreedyScheduling)
            {
                // mj+greedy setups
                return ParallelDomainBased_MiniJoinsGreedy_ForwardScanBased_PlaneSweep_Unrolled(pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads);
            }
            else
            {
                // mj+atomic setups
                return ParallelDomainBased_MiniJoinsAtomic_ForwardScanBased_PlaneSweep_Unrolled(pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads);
            }
        }
        else
        {
            // atomic setups
            return ParallelDomainBased_Atomic_ForwardScanBased_PlaneSweep_Unrolled(pR, pS, prR, prS, runNumPartitionsPerRelation, runNumThreads);
        }
    }
    else
    {
        if (runMiniJoinsBreakDown)
        {
            if (runGreedyScheduling)
            {
                // mj+greedy setups
                return ParallelDomainBased_MiniJoinsGreedy_ForwardScanBased_PlaneSweep_Rolled(pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads);
            }
            else
            {
                // mj+atomic setups
                return ParallelDomainBased_MiniJoinsAtomic_ForwardScanBased_PlaneSweep_Rolled(pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads);
            }
        }
        else
        {
            // atomic setups
            return ParallelDomainBased_Atomic_ForwardScanBased_PlaneSweep_Rolled(pR, pS, prR, prS, runNumPartitionsPerRelation, runNumThreads);
        }
    }
}
