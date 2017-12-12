#include "../def.h"
#include "../containers/relation.h"



unsigned long long ForwardScanBased_PlaneSweep_Rolled(Relation &R, Relation &S)
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
            auto pivot = s;
            while ((pivot < lastS) && (r->end >= pivot->start))
            {
                result += r->start ^ pivot->start;
                pivot++;
            }
            r++;
        }
        else
        {
            auto pivot = r;
            while ((pivot < lastR) && (s->end >= pivot->start))
            {
                result += pivot->start ^ s->start;
                pivot++;
            }
            s++;
        }
    }
    
    
    return result;
}


unsigned long long ForwardScanBased_PlaneSweep_Unrolled(Relation &R, Relation &S)
{
    unsigned long long numResults = 0;
    auto r = R.begin();
    auto s = S.begin();
    auto lastR = R.end();
    auto lastS = S.end();
    
    
    while ((r < lastR) && (s < lastS))
    {
        if (*r < *s)
        {
            auto pivot = s;
            while ((lastS-pivot >= 32) && (r->end >= (pivot+31)->start))
            {
                numResults += r->start ^ (pivot+0)->start;
                numResults += r->start ^ (pivot+1)->start;
                numResults += r->start ^ (pivot+2)->start;
                numResults += r->start ^ (pivot+3)->start;
                numResults += r->start ^ (pivot+4)->start;
                numResults += r->start ^ (pivot+5)->start;
                numResults += r->start ^ (pivot+6)->start;
                numResults += r->start ^ (pivot+7)->start;
                numResults += r->start ^ (pivot+8)->start;
                numResults += r->start ^ (pivot+9)->start;
                numResults += r->start ^ (pivot+10)->start;
                numResults += r->start ^ (pivot+11)->start;
                numResults += r->start ^ (pivot+12)->start;
                numResults += r->start ^ (pivot+13)->start;
                numResults += r->start ^ (pivot+14)->start;
                numResults += r->start ^ (pivot+15)->start;
                numResults += r->start ^ (pivot+16)->start;
                numResults += r->start ^ (pivot+17)->start;
                numResults += r->start ^ (pivot+18)->start;
                numResults += r->start ^ (pivot+19)->start;
                numResults += r->start ^ (pivot+20)->start;
                numResults += r->start ^ (pivot+21)->start;
                numResults += r->start ^ (pivot+22)->start;
                numResults += r->start ^ (pivot+23)->start;
                numResults += r->start ^ (pivot+24)->start;
                numResults += r->start ^ (pivot+25)->start;
                numResults += r->start ^ (pivot+26)->start;
                numResults += r->start ^ (pivot+27)->start;
                numResults += r->start ^ (pivot+28)->start;
                numResults += r->start ^ (pivot+29)->start;
                numResults += r->start ^ (pivot+30)->start;
                numResults += r->start ^ (pivot+31)->start;
                pivot += 32;
            }
            
            if ((lastS-pivot >= 16) && (r->end >= (pivot+15)->start))
            {
                numResults += r->start ^ (pivot+0)->start;
                numResults += r->start ^ (pivot+1)->start;
                numResults += r->start ^ (pivot+2)->start;
                numResults += r->start ^ (pivot+3)->start;
                numResults += r->start ^ (pivot+4)->start;
                numResults += r->start ^ (pivot+5)->start;
                numResults += r->start ^ (pivot+6)->start;
                numResults += r->start ^ (pivot+7)->start;
                numResults += r->start ^ (pivot+8)->start;
                numResults += r->start ^ (pivot+9)->start;
                numResults += r->start ^ (pivot+10)->start;
                numResults += r->start ^ (pivot+11)->start;
                numResults += r->start ^ (pivot+12)->start;
                numResults += r->start ^ (pivot+13)->start;
                numResults += r->start ^ (pivot+14)->start;
                numResults += r->start ^ (pivot+15)->start;
                pivot += 16;
            }
            
            if ((lastS-pivot >= 8) && (r->end >= (pivot+7)->start))
            {
                numResults += r->start ^ (pivot+0)->start;
                numResults += r->start ^ (pivot+1)->start;
                numResults += r->start ^ (pivot+2)->start;
                numResults += r->start ^ (pivot+3)->start;
                numResults += r->start ^ (pivot+4)->start;
                numResults += r->start ^ (pivot+5)->start;
                numResults += r->start ^ (pivot+6)->start;
                numResults += r->start ^ (pivot+7)->start;
                pivot += 8;
            }
            
            if ((lastS-pivot >= 4) && (r->end >= (pivot+3)->start))
            {
                numResults += r->start ^ (pivot+0)->start;
                numResults += r->start ^ (pivot+1)->start;
                numResults += r->start ^ (pivot+2)->start;
                numResults += r->start ^ (pivot+3)->start;
                pivot += 4;
            }
            
            while ((pivot < lastS) && (r->end >= pivot->start))
            {
                numResults += r->start ^ pivot->start; pivot++;
            }
            
            ++r;
        }
        else
        {
            auto pivot = r;
            while ((lastR-pivot >= 32) && (s->end >= (pivot+31)->start))
            {
                numResults += (pivot+0)->start ^ s->start;
                numResults += (pivot+1)->start ^ s->start;
                numResults += (pivot+2)->start ^ s->start;
                numResults += (pivot+3)->start ^ s->start;
                numResults += (pivot+4)->start ^ s->start;
                numResults += (pivot+5)->start ^ s->start;
                numResults += (pivot+6)->start ^ s->start;
                numResults += (pivot+7)->start ^ s->start;
                numResults += (pivot+8)->start ^ s->start;
                numResults += (pivot+9)->start ^ s->start;
                numResults += (pivot+10)->start ^ s->start;
                numResults += (pivot+11)->start ^ s->start;
                numResults += (pivot+12)->start ^ s->start;
                numResults += (pivot+13)->start ^ s->start;
                numResults += (pivot+14)->start ^ s->start;
                numResults += (pivot+15)->start ^ s->start;
                numResults += (pivot+16)->start ^ s->start;
                numResults += (pivot+17)->start ^ s->start;
                numResults += (pivot+18)->start ^ s->start;
                numResults += (pivot+19)->start ^ s->start;
                numResults += (pivot+20)->start ^ s->start;
                numResults += (pivot+21)->start ^ s->start;
                numResults += (pivot+22)->start ^ s->start;
                numResults += (pivot+23)->start ^ s->start;
                numResults += (pivot+24)->start ^ s->start;
                numResults += (pivot+25)->start ^ s->start;
                numResults += (pivot+26)->start ^ s->start;
                numResults += (pivot+27)->start ^ s->start;
                numResults += (pivot+28)->start ^ s->start;
                numResults += (pivot+29)->start ^ s->start;
                numResults += (pivot+30)->start ^ s->start;
                numResults += (pivot+31)->start ^ s->start;
                pivot += 32;
            }
            
            if ((lastR-pivot >= 16) && (s->end >= (pivot+15)->start))
            {
                numResults += (pivot+0)->start ^ s->start;
                numResults += (pivot+1)->start ^ s->start;
                numResults += (pivot+2)->start ^ s->start;
                numResults += (pivot+3)->start ^ s->start;
                numResults += (pivot+4)->start ^ s->start;
                numResults += (pivot+5)->start ^ s->start;
                numResults += (pivot+6)->start ^ s->start;
                numResults += (pivot+7)->start ^ s->start;
                numResults += (pivot+8)->start ^ s->start;
                numResults += (pivot+9)->start ^ s->start;
                numResults += (pivot+10)->start ^ s->start;
                numResults += (pivot+11)->start ^ s->start;
                numResults += (pivot+12)->start ^ s->start;
                numResults += (pivot+13)->start ^ s->start;
                numResults += (pivot+14)->start ^ s->start;
                numResults += (pivot+15)->start ^ s->start;
                pivot += 16;
            }
            
            if ((lastR-pivot >= 8) && (s->end >= (pivot+7)->start))
            {
                numResults += (pivot+0)->start ^ s->start;
                numResults += (pivot+1)->start ^ s->start;
                numResults += (pivot+2)->start ^ s->start;
                numResults += (pivot+3)->start ^ s->start;
                numResults += (pivot+4)->start ^ s->start;
                numResults += (pivot+5)->start ^ s->start;
                numResults += (pivot+6)->start ^ s->start;
                numResults += (pivot+7)->start ^ s->start;
                pivot += 8;
            }
            
            if ((lastR-pivot >= 4) && (s->end >= (pivot+3)->start))
            {
                numResults += (pivot+0)->start ^ s->start;
                numResults += (pivot+1)->start ^ s->start;
                numResults += (pivot+2)->start ^ s->start;
                numResults += (pivot+3)->start ^ s->start;
                pivot += 4;
            }
            
            while ((pivot < lastR) && (s->end >= pivot->start))
            {
                numResults += pivot->start ^ s->start; pivot++;
            }
            
            ++s;
        }
    }
    
    
    return numResults;
}


unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Rolled(Relation *pR, Relation *pS, int runNumThreads)
{
    unsigned long long result = 0;
    int n = sqrt(runNumThreads);
    
    
    #pragma omp parallel for num_threads(runNumThreads) collapse(2) reduction(+ : result)
    for (int i = 0; i < n; i++)
    {
        for (int j = 0; j < n; j++)
        {
            result += ForwardScanBased_PlaneSweep_Rolled(pR[i], pS[j]);
        }
    }
    
    
    return result;
}


unsigned long long ParallelHashBased_ForwardScanBased_PlaneSweep_Unrolled(Relation *pR, Relation *pS, int runNumThreads)
{
    unsigned long long result = 0;
    int n = sqrt(runNumThreads);
    
    
    #pragma omp parallel for num_threads(runNumThreads) collapse(2) reduction(+ : result)
    for (int i = 0; i < n; i++)
    {
        for (int j = 0; j < n; j++)
        {
            result += ForwardScanBased_PlaneSweep_Unrolled(pR[i], pS[j]);
        }
    }
    
    
    return result;
}
