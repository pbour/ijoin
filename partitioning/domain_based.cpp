#include "../def.h"
#include "../containers/relation.h"
#include "../containers/bucket_index.h"



// Very simple load estimation functions but they work very well in practice.
inline size_t ComputeLoad(int pid, vector<size_t> &curr_sR, vector<size_t> &curr_sS)
{
    return curr_sR[pid]*curr_sS[pid];
}


inline size_t ComputeIncreasedNewLoad(int pid, vector<size_t> &curr_sR, vector<size_t> &curr_sS, vector<size_t> &shistR, vector<size_t> &shistS, int gid)
{
    return (curr_sR[pid]+shistR[gid])*(curr_sS[pid]+shistS[gid]);
}


inline size_t ComputeDecreasedNewLoad(int pid, vector<size_t> &curr_sR, vector<size_t> &curr_sS, vector<size_t> &shistR, vector<size_t> &shistS, int gid)
{
    return (curr_sR[pid]-shistR[gid])*(curr_sS[pid]-shistS[gid]);
}


class mycomparison
{
public:
    bool operator() (const pair<int, size_t>& lhs, const pair<int, size_t>& rhs) const
    {
        return (rhs.second > lhs.second);
    }
} myobject;




inline void CountPartitionContents(const Relation& R, size_t *pR_size, size_t *prR_size, Timestamp gmin, Timestamp gmax, Timestamp partitionExtent, int runNumPartitionsPerRelation)
{
    for (const auto& r : R)
    {
        auto i = (r.start == 0) ? 0 : r.start/partitionExtent;
        auto j = (r.end == gmax) ? (runNumPartitionsPerRelation-1) : r.end/partitionExtent;
        
        if (r.start == gmax)
            i = runNumPartitionsPerRelation-1;
        pR_size[i]++;
        while (i != j)
        {
            i++;
            prR_size[i]++;
        }
    }
}


inline void CountPartitionContents(const Relation& R, size_t *pR_size, size_t *prR_size, size_t *prfR_size, Timestamp gmin, Timestamp gmax, Timestamp partitionExtent, int runNumPartitionsPerRelation)
{
    for (const auto& r : R)
    {
        auto i = (r.start == 0) ? 0 : r.start/partitionExtent;
        auto j = (r.end == gmax) ? (runNumPartitionsPerRelation-1) : r.end/partitionExtent;
        
        if (r.start == gmax)
            i = runNumPartitionsPerRelation-1;
            pR_size[i]++;
        while (i != j)
        {
            i++;
            if (i < j)
                prfR_size[i]++;
            else
                prR_size[i]++;
        }
    }
}


inline void CountPartitionContents(const Relation& R, size_t *pR_size, size_t *prR_size, vector<Timestamp> &boundaries, int runNumPartitionsPerRelation)
{
    for (const auto& r : R)
    {
        auto i = 0;
        while ((i < runNumPartitionsPerRelation) && (boundaries[i] < r.start))
            i++;
        if (i == runNumPartitionsPerRelation)
            i = runNumPartitionsPerRelation-1;
            pR_size[i]++;
        
        i++;
        while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < r.end))
        {
            prR_size[i]++;
            i++;
        }
    }
}


inline void CountPartitionContents(const Relation& R, size_t *pR_size, size_t *prR_size, size_t *prfR_size, vector<Timestamp> &boundaries, int runNumPartitionsPerRelation)
{
    for (const auto& r : R)
    {
        auto i = 0;
        while ((i < runNumPartitionsPerRelation) && (boundaries[i] < r.start))
            i++;
        if (i == runNumPartitionsPerRelation)
            i = runNumPartitionsPerRelation-1;
            pR_size[i]++;
        
        i++;
        while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < r.end))
        {
            if ((boundaries[i] < r.end) && (i != runNumPartitionsPerRelation-1))
                prfR_size[i]++;
            else
                prR_size[i]++;
            i++;
        }
    }
}


inline void AllocateMemoryForPartitions(Relation *pR, Relation *prR, size_t *pR_size, size_t *prR_size, int runNumPartitionsPerRelation)
{
    for (int i = 0; i < runNumPartitionsPerRelation; i++)
    {
        pR[i].reserve(pR_size[i]);
        prR[i].reserve(prR_size[i]);
    }
}


inline void AllocateMemoryForPartitions(Relation *pR, Relation *prR, Relation *prfR, size_t *pR_size, size_t *prR_size, size_t *prfR_size, int runNumPartitionsPerRelation)
{
    for (int i = 0; i < runNumPartitionsPerRelation; i++)
    {
        pR[i].reserve(pR_size[i]);
        prR[i].reserve(prR_size[i]);
        prfR[i].reserve(prfR_size[i]);
    }

}


inline void FillPartitions(const Relation& R, Relation *pR, Relation *prR, Timestamp gmax, Timestamp partitionExtent, int runNumPartitionsPerRelation)
{
    for (const auto& r : R)
    {
        auto i = (r.start == 0) ? 0 : r.start/partitionExtent;
        auto j = (r.end == gmax) ? (runNumPartitionsPerRelation-1) : r.end/partitionExtent;
        
        if (r.start == gmax)
            i = runNumPartitionsPerRelation-1;
        pR[i].push_back(r);

        while (i != j)
        {
            i++;
            prR[i].push_back(r);
        }
    }
}


inline void FillPartitions(const Relation& R, Relation *pR, Relation *prR, vector<Timestamp> &boundaries, int runNumPartitionsPerRelation)
{
    for (const auto& r : R)
    {
        auto i = 0;
        while ((i < runNumPartitionsPerRelation) && (boundaries[i] < r.start))
            i++;
        if (i == runNumPartitionsPerRelation)
            i = runNumPartitionsPerRelation-1;
        pR[i].push_back(r);
        
        i++;
        while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < r.end))
        {
            prR[i].push_back(r);
            i++;
        }
    }
}


inline void FillPartitions(const Relation& R, Relation *pR, Relation *prR, Relation *prfR, Timestamp gmax, Timestamp partitionExtent, int runNumPartitionsPerRelation)
{
    for (const auto& r : R)
    {
        auto i = (r.start == 0) ? 0 : r.start/partitionExtent;
        auto j = (r.end == gmax) ? (runNumPartitionsPerRelation-1) : r.end/partitionExtent;
        
        if (r.start == gmax)
            i = runNumPartitionsPerRelation-1;
        pR[i].push_back(r);

        while (i != j)
        {
            i++;
            if (i < j)
                prfR[i].push_back(r);
            else
                prR[i].push_back(r);
        }
    }
}


inline void FillPartitions(const Relation& R, Relation *pR, Relation *prR, Relation *prfR, vector<Timestamp> &boundaries, int runNumPartitionsPerRelation)
{
    for (const auto& r : R)
    {
        auto i = 0;
        while ((i < runNumPartitionsPerRelation) && (boundaries[i] < r.start))
            i++;
        if (i == runNumPartitionsPerRelation)
            i = runNumPartitionsPerRelation-1;
        pR[i].push_back(r);
            
        i++;
        while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < r.end))
        {
            if ((boundaries[i] < r.end) && (i != runNumPartitionsPerRelation-1))
                prfR[i].push_back(r);
            else
                prR[i].push_back(r);
            i++;
        }
    }
}


inline void BuildStartHistograms(const Relation& R, vector<size_t> &shistR, Timestamp gmax, Timestamp granuleExtent, int numGranules)
{
    for (const auto& r : R)
    {
        auto i = (r.start == 0) ? 0 : r.start/granuleExtent;
        auto j = (r.end == gmax) ? (numGranules-1) : r.end/granuleExtent;

        if (r.start == gmax)
            i = numGranules-1;

        shistR[i]++;
    }
}


inline void AdaptivePartitioning(vector<size_t> &shistR, vector<size_t> &shistS, vector<Timestamp> &boundaries, Timestamp gmax, Timestamp granuleExtent, int numGranules, int runNumPartitionsPerRelation)
{
    vector<int> sbboundaries(runNumPartitionsPerRelation, 0);
    vector<size_t> curr(runNumPartitionsPerRelation, 0);
    vector<size_t> curr_sR(runNumPartitionsPerRelation, 0), curr_sS(runNumPartitionsPerRelation, 0);
    int curr_part = 0, k = 0;
    int numGranuelsPerPartition = numGranules/runNumPartitionsPerRelation;
    auto prev_sp = -1;
    auto checked = 0, updated = 0, prev_updated = 0;


    // Compute initial load per partition.
    for (int p = 0; p < runNumPartitionsPerRelation; p++)
    {
        for (int g = 0; g < numGranuelsPerPartition; g++)
        {
            auto k = p*numGranuelsPerPartition + g;
            curr_sR[p] += shistR[k];
            curr_sS[p] += shistS[k];
        }
        boundaries[p] = ((p+1)*numGranuelsPerPartition)*granuleExtent;
        sbboundaries[p] = p*numGranuelsPerPartition;
        curr[p] = ComputeLoad(p, curr_sR, curr_sS);
    }
    boundaries[runNumPartitionsPerRelation-1] = gmax;
    
    vector<pair<int, size_t> > Q;
    auto idx = -1;
    while (checked-updated < runNumPartitionsPerRelation)
    {
        int sp = 0, cp = 0;
        if (idx == -1)
        {
            Q.clear();
            for (int p = 0; p < runNumPartitionsPerRelation; p++)
                Q.push_back(make_pair(p, curr[p]));
                sort(Q.begin(), Q.end(), myobject);
                idx = runNumPartitionsPerRelation-1;
                }
        sp = Q[idx].first;
        idx--;
        checked++;
        
        // Move load to the previous partition.
        if (sp == runNumPartitionsPerRelation-1 || sp != 0 && curr[sp-1] < curr[sp+1])
        {
            auto gid = sbboundaries[sp];
            auto newSP = ComputeDecreasedNewLoad(sp, curr_sR, curr_sS, shistR, shistS, gid);
            auto newSP1 = ComputeIncreasedNewLoad((sp-1), curr_sR, curr_sS, shistR, shistS, gid);
            
            while ((gid != numGranules-1) && (newSP > newSP1))
            {
                curr_sR[sp] -= shistR[gid];
                curr_sS[sp] -= shistS[gid];
                curr[sp] = newSP;
                curr_sR[sp-1] += shistR[gid];
                curr_sS[sp-1] += shistS[gid];
                curr[sp-1] = newSP1;
                boundaries[sp-1] += granuleExtent;
                sbboundaries[sp]++;
                
                gid++;
                newSP = ComputeDecreasedNewLoad(sp, curr_sR, curr_sS, shistR, shistS, gid);
                newSP1 = ComputeIncreasedNewLoad((sp-1), curr_sR, curr_sS, shistR, shistS, gid);
                updated = checked;
            }
        }
        // Move load to the next partition.
        else if (sp == 0 || curr[sp-1] > curr[sp+1])
        {
            auto gid = sbboundaries[sp+1]-1;
            auto newSP = ComputeDecreasedNewLoad(sp, curr_sR, curr_sS, shistR, shistS, gid);
            auto newSP1 = ComputeIncreasedNewLoad((sp+1), curr_sR, curr_sS, shistR, shistS, gid);
            
            while ((gid != -1) && (newSP > newSP1))
            {
                curr_sR[sp] -= shistR[gid];
                curr_sS[sp] -= shistS[gid];
                curr[sp] = newSP;
                curr_sR[sp+1] += shistR[gid];
                curr_sS[sp+1] += shistS[gid];
                curr[sp+1] = newSP1;
                boundaries[sp] -= granuleExtent;
                sbboundaries[sp+1]--;
                
                gid--;
                newSP = ComputeDecreasedNewLoad(sp, curr_sR, curr_sS, shistR, shistS, gid);
                newSP1 = ComputeIncreasedNewLoad((sp+1), curr_sR, curr_sS, shistR, shistS, gid);
                updated = checked;
            }
        }
    }
}



////////////////////////////
// Without BucketIndexing //
////////////////////////////
// For the atomic/uniform setup.
void ParallelDomainBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, int runNumPartitionsPerRelation, int runNumThreads)
{
    Timestamp gmin = std::min(R.minStart, S.minStart), gmax = std::max(R.maxEnd, S.maxEnd);
    auto partitionExtent = (Timestamp)ceil((double)(gmax-gmin)/runNumPartitionsPerRelation);
    size_t *pR_size, *pS_size, *prR_size, *prS_size;


    // Initiliaze auxliary counter.
    pR_size = new size_t[runNumPartitionsPerRelation];
    pS_size = new size_t[runNumPartitionsPerRelation];
    prR_size = new size_t[runNumPartitionsPerRelation];
    prS_size = new size_t[runNumPartitionsPerRelation];
    memset(pR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(pS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));

    // Partition relations.
    #pragma omp parallel num_threads(runNumThreads)
    {
        #pragma omp sections
        {
            // Step 1: create and fill structures for each partition.
            // Use two parallel threads, one for each input relation.
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(R, pR_size, prR_size, gmin, gmax, partitionExtent, runNumPartitionsPerRelation);

                // Allocate necessary memory.
                AllocateMemoryForPartitions(pR, prR, pR_size, prR_size, runNumPartitionsPerRelation);

                // Fill structures on each partition.
                FillPartitions(R, pR, prR, gmax, partitionExtent, runNumPartitionsPerRelation);
            }
            
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(S, pS_size, prS_size, gmin, gmax, partitionExtent, runNumPartitionsPerRelation);

                // Allocate necessary memory.
                AllocateMemoryForPartitions(pS, prS, pS_size, prS_size, runNumPartitionsPerRelation);

                // Fill structures on each partition.
                FillPartitions(S, pS, prS, gmax, partitionExtent, runNumPartitionsPerRelation);
            }
        }
        #pragma omp barrier
        

        // Step 2: sort partitions; in practice we only to sort structures that contain exclusively original records, i.e., no replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        sort(pR[i].begin(), pR[i].end());
                        break;
                        
                    case 1:
                        sort(pS[i].begin(), pS[i].end());
                        break;
                }
            }
        }
    }

    // Free allocated memory.
    delete[] pR_size;
    delete[] pS_size;
    delete[] prR_size;
    delete[] prS_size;
}


// For the mj+atomic/uniform or the mj+greedy/uniform setup.
void ParallelDomainBased_Partition_MiniJoinsBreakdown(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads)
{
    Timestamp gmin = std::min(R.minStart, S.minStart), gmax = std::max(R.maxEnd, S.maxEnd);
    auto partitionExtent = (Timestamp)ceil((double)(gmax-gmin)/runNumPartitionsPerRelation);
    size_t *pR_size, *pS_size, *prR_size, *prS_size, *prfR_size, *prfS_size;
    
    
    // Initiliaze auxliary counter
    pR_size = new size_t[runNumPartitionsPerRelation];
    pS_size = new size_t[runNumPartitionsPerRelation];
    prR_size = new size_t[runNumPartitionsPerRelation];
    prS_size = new size_t[runNumPartitionsPerRelation];
    prfR_size = new size_t[runNumPartitionsPerRelation];
    prfS_size = new size_t[runNumPartitionsPerRelation];
    memset(pR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(pS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prfR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prfS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));

    // Partition relations.
    #pragma omp parallel num_threads(runNumThreads)
    {
        #pragma omp sections
        {
            // Step 1: create and fill structures for each partition.
            // Use two parallel threads, one for each input relation.
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(R, pR_size, prR_size, prfR_size, gmin, gmax, partitionExtent, runNumPartitionsPerRelation);

                // Allocate necessary memory.
                AllocateMemoryForPartitions(pR, prR, prfR, pR_size, prR_size, prfR_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                FillPartitions(R, pR, prR, prfR, gmax, partitionExtent, runNumPartitionsPerRelation);
            }
            
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(S, pS_size, prS_size, prfS_size, gmin, gmax, partitionExtent, runNumPartitionsPerRelation);
                
                // Allocate necessary memory.
                AllocateMemoryForPartitions(pS, prS, prfS, pS_size, prS_size, prfS_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                FillPartitions(S, pS, prS, prfS, gmax, partitionExtent, runNumPartitionsPerRelation);
            }
        }
        #pragma omp barrier
        

        // Step 2: sort partitions; in practice we only to sort structures that contain exclusively original records, i.e., no replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        sort(pR[i].begin(), pR[i].end());
                        break;
                        
                    case 1:
                        sort(pS[i].begin(), pS[i].end());
                        break;
                }
            }
        }
    }

    // Free allocated memory.
    delete[] pR_size;
    delete[] pS_size;
    delete[] prR_size;
    delete[] prS_size;
    delete[] prfR_size;
    delete[] prfS_size;
}


// For the atomic/adaptive setup.
void ParallelDomainBased_Partition_Adaptive(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, int runNumPartitionsPerRelation, int runNumThreads)
{
    int numGranules = ((runNumPartitionsPerRelation < 10)? runNumPartitionsPerRelation*1000: runNumPartitionsPerRelation*100);
    Timestamp gmin = std::min(R.minStart, S.minStart), gmax = std::max(R.maxEnd, S.maxEnd);
    auto granuleExtent = (Timestamp)ceil((double)(gmax-gmin)/numGranules);
    vector<size_t> shistR(numGranules, 0), shistS(numGranules, 0);
    vector<Timestamp> boundaries(runNumPartitionsPerRelation, 0);
    size_t *pR_size, *pS_size, *prR_size, *prS_size;
    
    
    // Initiliaze auxliary counter
    pR_size = new size_t[runNumPartitionsPerRelation];
    pS_size = new size_t[runNumPartitionsPerRelation];
    prR_size = new size_t[runNumPartitionsPerRelation];
    prS_size = new size_t[runNumPartitionsPerRelation];
    memset(pR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(pS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));

    // Partition relations.
    #pragma omp parallel num_threads(runNumThreads)
    {
        // Step 1: build histogram statistics for granules; i.e., count the records starting inside each granule
        // Use two parallel threads; one for each input relation.
        #pragma omp sections
        {
            #pragma omp section
            {
                BuildStartHistograms(R, shistR, gmax, granuleExtent, numGranules);
            }
            
            #pragma omp section
            {
                BuildStartHistograms(S, shistS, gmax, granuleExtent, numGranules);
            }
        }
        #pragma omp barrier

        
        // Step 2: reposition the boundaries between partitions; initial partitioning includes the same number of granules inside each partition.
        // Use one thread.
        #pragma omp single
        {
            AdaptivePartitioning(shistR, shistS, boundaries, gmax, granuleExtent, numGranules, runNumPartitionsPerRelation);
        }


        // Step 3: create and fill structures for each partition.
        // Use two parallel threads, one for each input relation.
        #pragma omp sections
        {
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(R, pR_size, prR_size, boundaries, runNumPartitionsPerRelation);

                // Allocate necessary memory.
                AllocateMemoryForPartitions(pR, prR, pR_size, prR_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                FillPartitions(R, pR, prR, boundaries, runNumPartitionsPerRelation);
            }
            
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(S, pS_size, prS_size, boundaries, runNumPartitionsPerRelation);

                // Allocate necessary memory.
                AllocateMemoryForPartitions(pS, prS, pS_size, prS_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                FillPartitions(S, pS, prS, boundaries, runNumPartitionsPerRelation);
            }
        }
        #pragma omp barrier
        

        // Step 4: sort partitions; in practice we only to sort structures that contain exclusively original records, i.e., no replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        sort(pR[i].begin(), pR[i].end());
                        break;
                        
                    case 1:
                        sort(pS[i].begin(), pS[i].end());
                        break;
                }
            }
        }
    }
    
    // Free allocated memory.
    delete[] pR_size;
    delete[] pS_size;
    delete[] prR_size;
    delete[] prS_size;
}


// For the mj+atomic/adaptive or the mj+greedy/adaptive setup.
void ParallelDomainBased_Partition_MiniJoinsBreakdown_Adaptive(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads)
{
    int numGranules = ((runNumPartitionsPerRelation < 10)? runNumPartitionsPerRelation*1000: runNumPartitionsPerRelation*100);
    Timestamp gmin = std::min(R.minStart, S.minStart), gmax = std::max(R.maxEnd, S.maxEnd);
    auto granuleExtent = (Timestamp)ceil((double)(gmax-gmin)/numGranules);
    vector<size_t> shistR(numGranules, 0), shistS(numGranules, 0);
    vector<Timestamp> boundaries(runNumPartitionsPerRelation, 0);
    size_t *pR_size, *pS_size, *prR_size, *prS_size, *prfR_size, *prfS_size;
    
    
    // Initiliaze auxliary counter
    pR_size = new size_t[runNumPartitionsPerRelation];
    pS_size = new size_t[runNumPartitionsPerRelation];
    prR_size = new size_t[runNumPartitionsPerRelation];
    prS_size = new size_t[runNumPartitionsPerRelation];
    prfR_size = new size_t[runNumPartitionsPerRelation];
    prfS_size = new size_t[runNumPartitionsPerRelation];
    memset(pR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(pS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prfR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prfS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));

    // Partition relations.
    #pragma omp parallel num_threads(runNumThreads)
    {
        // Step 1: build histogram statistics for granules; i.e., count the records starting inside each granule
        // Use two parallel threads; one for each input relation.
        #pragma omp sections
        {
            #pragma omp section
            {
                BuildStartHistograms(R, shistR, gmax, granuleExtent, numGranules);
            }
            
            #pragma omp section
            {
                BuildStartHistograms(S, shistS, gmax, granuleExtent, numGranules);
            }
        }
        #pragma omp barrier

        
        // Step 2: reposition the boundaries between partitions; initial partitioning includes the same number of granules inside each partition.
        // Use one thread.
        #pragma omp single
        {
            AdaptivePartitioning(shistR, shistS, boundaries, gmax, granuleExtent, numGranules, runNumPartitionsPerRelation);
        }
        
        
        // Step 3: create and fill structures for each partition.
        // Use two parallel threads, one for each input relation.
        #pragma omp sections
        {
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(R, pR_size, prR_size, prfR_size, boundaries, runNumPartitionsPerRelation);

                // Allocate necessary memory.
                AllocateMemoryForPartitions(pR, prR, prfR, pR_size, prR_size, prfR_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                FillPartitions(R, pR, prR, prfR, boundaries, runNumPartitionsPerRelation);
            }
            
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(S, pS_size, prS_size, prfS_size, boundaries, runNumPartitionsPerRelation);

                // Allocate necessary memory.
                AllocateMemoryForPartitions(pS, prS, prfS, pS_size, prS_size, prfS_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                FillPartitions(S, pS, prS, prfS, boundaries, runNumPartitionsPerRelation);
            }
        }
        #pragma omp barrier
        
        
        // Step 4: sort partitions; in practice we only to sort structures that contain exclusively original records, i.e., replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        sort(pR[i].begin(), pR[i].end());
                        break;
                        
                    case 1:
                        sort(pS[i].begin(), pS[i].end());
                        break;
                }
            }
        }
    }

    // Free allocated memory.
    delete[] pR_size;
    delete[] pS_size;
    delete[] prR_size;
    delete[] prS_size;
    delete[] prfR_size;
    delete[] prfS_size;
}


void ParallelDomainBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, int runNumPartitionsPerRelation, int runNumThreads, bool runMiniJoinsBreakdown, bool runAdaptivePartitioning)
{
    if (runMiniJoinsBreakdown)
    {
        if (runAdaptivePartitioning)
            ParallelDomainBased_Partition_MiniJoinsBreakdown_Adaptive(R, S, pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads);
        else
            ParallelDomainBased_Partition_MiniJoinsBreakdown(R, S, pR, pS, prR, prS, prfR, prfS, runNumPartitionsPerRelation, runNumThreads);
    }
    else
    {
        if (runAdaptivePartitioning)
            ParallelDomainBased_Partition_Adaptive(R, S, pR, pS, prR, prS, runNumPartitionsPerRelation, runNumThreads);
        else
            ParallelDomainBased_Partition(R, S, pR, pS, prR, prS, runNumPartitionsPerRelation, runNumThreads);
    }
}



/////////////////////////
// With BucketIndexing //
/////////////////////////
// For the atomic/uniform setup.
void ParallelDomainBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumBuckets, int runNumPartitionsPerRelation, int runNumThreads)
{
    Timestamp gmin = std::min(R.minStart, S.minStart), gmax = std::max(R.maxEnd, S.maxEnd);
    auto partitionExtent = (Timestamp)ceil((double)(gmax-gmin)/runNumPartitionsPerRelation);
    size_t *pR_size, *pS_size, *prR_size, *prS_size;
    
    
    // Initiliaze auxliary counter
    pR_size = new size_t[runNumPartitionsPerRelation];
    pS_size = new size_t[runNumPartitionsPerRelation];
    prR_size = new size_t[runNumPartitionsPerRelation];
    prS_size = new size_t[runNumPartitionsPerRelation];
    memset(pR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(pS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    
    // Partition relations.
    #pragma omp parallel num_threads(runNumThreads)
    {
        #pragma omp sections
        {
            // Step 1: create and fill structures for each partition.
            // Use two parallel threads, one for each input relation.
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(R, pR_size, prR_size, gmin, gmax, partitionExtent, runNumPartitionsPerRelation);
                
                // Allocate necessary memory.
                AllocateMemoryForPartitions(pR, prR, pR_size, prR_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                for (const auto& r : R)
                {
                    auto i = (r.start == 0) ? 0 : r.start/partitionExtent;
                    auto j = (r.end == gmax) ? (runNumPartitionsPerRelation-1) : r.end/partitionExtent;
                    
                    if (r.start == gmax)
                        i = runNumPartitionsPerRelation-1;
                    pR[i].push_back(r);
                    pR[i].minStart = std::min(pR[i].minStart, r.start);
                    pR[i].maxStart = std::max(pR[i].maxStart, r.start);
                    pR[i].minEnd   = std::min(pR[i].minEnd  , r.end);
                    pR[i].maxEnd   = std::max(pR[i].maxEnd  , r.end);
                    while (i != j)
                    {
                        i++;
                        prR[i].push_back(r);
                    }
                }
            }
            
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(S, pS_size, prS_size, gmin, gmax, partitionExtent, runNumPartitionsPerRelation);
                
                // Allocate necessary memory.
                AllocateMemoryForPartitions(pS, prS, pS_size, prS_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                for (const auto& s : S)
                {
                    auto i = (s.start == 0) ? 0 : s.start/partitionExtent;
                    auto j = (s.end == gmax) ? (runNumPartitionsPerRelation-1) : s.end/partitionExtent;
                    
                    if (s.start == gmax)
                        i = runNumPartitionsPerRelation-1;
                    pS[i].push_back(s);
                    pS[i].minStart = std::min(pS[i].minStart, s.start);
                    pS[i].maxStart = std::max(pS[i].maxStart, s.start);
                    pS[i].minEnd   = std::min(pS[i].minEnd  , s.end);
                    pS[i].maxEnd   = std::max(pS[i].maxEnd  , s.end);
                    while (i != j)
                    {
                        i++;
                        prS[i].push_back(s);
                    }
                }
            }
        }
        #pragma omp barrier
        
        
        // Step 2: sort partitions; in practice, we only to sort structures that contain exclusively original records, i.e., no replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        sort(pR[i].begin(), pR[i].end());
                        break;
                        
                    case 1:
                        sort(pS[i].begin(), pS[i].end());
                        break;
                }
            }
        }
        #pragma omp barrier
        
        
        // Step 3: build bucket indices; in practice, we only to index original records, i.e., no replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        pBIR[i].build(pR[i], runNumBuckets);
                        break;
                        
                    case 1:
                        pBIS[i].build(pS[i], runNumBuckets);
                        break;
                }
            }
        }
    }
    
    // Free allocated memory.
    delete[] pR_size;
    delete[] pS_size;
    delete[] prR_size;
    delete[] prS_size;
}


// For the mj+atomic/uniform or the mj+greedy/uniform setup.
void ParallelDomainBased_Partition_MiniJoinsBreakdown(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumBuckets, int runNumPartitionsPerRelation, int runNumThreads)
{
    Timestamp gmin = std::min(R.minStart, S.minStart), gmax = std::max(R.maxEnd, S.maxEnd);
    auto partitionExtent = (Timestamp)ceil((double)(gmax-gmin)/runNumPartitionsPerRelation);
    size_t *pR_size, *pS_size, *prR_size, *prS_size, *prfR_size, *prfS_size;
    
    
    // Initiliaze auxliary counter
    pR_size = new size_t[runNumPartitionsPerRelation];
    pS_size = new size_t[runNumPartitionsPerRelation];
    prR_size = new size_t[runNumPartitionsPerRelation];
    prS_size = new size_t[runNumPartitionsPerRelation];
    prfR_size = new size_t[runNumPartitionsPerRelation];
    prfS_size = new size_t[runNumPartitionsPerRelation];
    memset(pR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(pS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prfR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prfS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));

    // Partition relations.
    #pragma omp parallel num_threads(runNumThreads)
    {
        #pragma omp sections
        {
            // Step 1: create and fill structures for each partition.
            // Use two parallel threads, one for each input relation.
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(R, pR_size, prR_size, prfR_size, gmin, gmax, partitionExtent, runNumPartitionsPerRelation);
                
                // Allocate necessary memory.
                AllocateMemoryForPartitions(pR, prR, prfR, pR_size, prR_size, prfR_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                for (const auto& r : R)
                {
                    auto i = (r.start == 0) ? 0 : r.start/partitionExtent;
                    auto j = (r.end == gmax) ? (runNumPartitionsPerRelation-1) : r.end/partitionExtent;
                    
                    if (r.start == gmax)
                        i = runNumPartitionsPerRelation-1;
                    pR[i].push_back(r);
                    pR[i].minStart = std::min(pR[i].minStart, r.start);
                    pR[i].maxStart = std::max(pR[i].maxStart, r.start);
                    pR[i].minEnd   = std::min(pR[i].minEnd  , r.end);
                    pR[i].maxEnd   = std::max(pR[i].maxEnd  , r.end);
                    while (i != j)
                    {
                        i++;
                        if (i < j)
                            prfR[i].push_back(r);
                        else
                            prR[i].push_back(r);
                    }
                }
            }
            
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(S, pS_size, prS_size, prfS_size, gmin, gmax, partitionExtent, runNumPartitionsPerRelation);
                
                // Allocate necessary memory.
                AllocateMemoryForPartitions(pS, prS, prfS, pS_size, prS_size, prfS_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                for (const auto& s : S)
                {
                    auto i = (s.start == 0) ? 0 : s.start/partitionExtent;
                    auto j = (s.end == gmax) ? (runNumPartitionsPerRelation-1) : s.end/partitionExtent;
                    
                    if (s.start == gmax)
                        i = runNumPartitionsPerRelation-1;
                    pS[i].push_back(s);
                    pS[i].minStart = std::min(pS[i].minStart, s.start);
                    pS[i].maxStart = std::max(pS[i].maxStart, s.start);
                    pS[i].minEnd   = std::min(pS[i].minEnd  , s.end);
                    pS[i].maxEnd   = std::max(pS[i].maxEnd  , s.end);
                    while (i != j)
                    {
                        i++;
                        if (i < j)
                            prfS[i].push_back(s);
                        else
                            prS[i].push_back(s);
                    }
                }
            }
        }
        #pragma omp barrier
        
        
        // Step 2: sort partitions; in practice we only to sort structures that contain exclusively original records, i.e., no replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        sort(pR[i].begin(), pR[i].end());
                        break;
                        
                    case 1:
                        sort(pS[i].begin(), pS[i].end());
                        break;
                }
            }
        }
        #pragma omp barrier

        
        // Step 3: build bucket indices; in practice, we only to index original records, i.e., no replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        pBIR[i].build(pR[i], runNumBuckets);
                        break;
                        
                    case 1:
                        pBIS[i].build(pS[i], runNumBuckets);
                        break;
                }
            }
        }
    }
    
    // Free allocated memory.
    delete[] pR_size;
    delete[] pS_size;
    delete[] prR_size;
    delete[] prS_size;
    delete[] prfR_size;
    delete[] prfS_size;
}


// For the atomic/adaptive setup.
void ParallelDomainBased_Partition_Adaptive(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumBuckets, int runNumPartitionsPerRelation, int runNumThreads)
{
    int numGranules = ((runNumPartitionsPerRelation < 10)? runNumPartitionsPerRelation*1000: runNumPartitionsPerRelation*100);
    Timestamp gmin = std::min(R.minStart, S.minStart), gmax = std::max(R.maxEnd, S.maxEnd);
    auto granuleExtent = (Timestamp)ceil((double)(gmax-gmin)/numGranules);
    vector<size_t> shistR(numGranules, 0), shistS(numGranules, 0);
    vector<Timestamp> boundaries(runNumPartitionsPerRelation, 0);
    size_t *pR_size, *pS_size, *prR_size, *prS_size;
    
    
    // Initiliaze auxliary counter
    pR_size = new size_t[runNumPartitionsPerRelation];
    pS_size = new size_t[runNumPartitionsPerRelation];
    prR_size = new size_t[runNumPartitionsPerRelation];
    prS_size = new size_t[runNumPartitionsPerRelation];
    memset(pR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(pS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    
    // Partition relations.
    #pragma omp parallel num_threads(runNumThreads)
    {
        // Step 1: build histogram statistics for granules; i.e., count the records starting inside each granule
        // Use two parallel threads; one for each input relation.
        #pragma omp sections
        {
            #pragma omp section
            {
                BuildStartHistograms(R, shistR, gmax, granuleExtent, numGranules);
            }
            
            #pragma omp section
            {
                BuildStartHistograms(S, shistS, gmax, granuleExtent, numGranules);
            }
        }
        #pragma omp barrier
        
        
        // Step 2: reposition the boundaries between partitions; initial partitioning includes the same number of granules inside each partition.
        // Use one thread.
        #pragma omp single
        {
            AdaptivePartitioning(shistR, shistS, boundaries, gmax, granuleExtent, numGranules, runNumPartitionsPerRelation);
        }
        
        
        // Step 3: create and fill structures for each partition.
        // Use two parallel threads, one for each input relation.
        #pragma omp sections
        {
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(R, pR_size, prR_size, boundaries, runNumPartitionsPerRelation);
                
                // Allocate necessary memory.
                AllocateMemoryForPartitions(pR, prR, pR_size, prR_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                for (const auto& r : R)
                {
                    auto i = 0;
                    while ((i < runNumPartitionsPerRelation) && (boundaries[i] < r.start))
                        i++;
                    if (i == runNumPartitionsPerRelation)
                        i = runNumPartitionsPerRelation-1;
                    pR[i].push_back(r);
                    pR[i].minStart = std::min(pR[i].minStart, r.start);
                    pR[i].maxStart = std::max(pR[i].maxStart, r.start);
                    pR[i].minEnd   = std::min(pR[i].minEnd  , r.end);
                    pR[i].maxEnd   = std::max(pR[i].maxEnd  , r.end);

                    i++;
                    while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < r.end))
                    {
                        prR[i].push_back(r);
                        i++;
                    }
                }
            }
            
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(S, pS_size, prS_size, boundaries, runNumPartitionsPerRelation);
                
                // Allocate necessary memory.
                AllocateMemoryForPartitions(pS, prS, pS_size, prS_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                for (const auto& s : S)
                {
                    auto i = 0;
                    while ((i < runNumPartitionsPerRelation) && (boundaries[i] < s.start))
                        i++;
                    if (i == runNumPartitionsPerRelation)
                        i = runNumPartitionsPerRelation-1;
                    pS[i].push_back(s);
                    pS[i].minStart = std::min(pS[i].minStart, s.start);
                    pS[i].maxStart = std::max(pS[i].maxStart, s.start);
                    pS[i].minEnd   = std::min(pS[i].minEnd  , s.end);
                    pS[i].maxEnd   = std::max(pS[i].maxEnd  , s.end);

                    i++;
                    while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < s.end))
                    {
                        prS[i].push_back(s);
                        i++;
                    }
                }
            }
        }
        #pragma omp barrier
        
        
        // Step 4: sort partitions; in practice we only to sort structures that contain exclusively original records, i.e., no replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        sort(pR[i].begin(), pR[i].end());
                        break;
                        
                    case 1:
                        sort(pS[i].begin(), pS[i].end());
                        break;
                }
            }
        }
        #pragma omp barrier

        
        // Step 5: build bucket indices; in practice, we only to index original records, i.e., no replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        pBIR[i].build(pR[i], runNumBuckets);
                        break;
                        
                    case 1:
                        pBIS[i].build(pS[i], runNumBuckets);
                        break;
                }
            }
        }
    }
    
    // Free allocated memory.
    delete[] pR_size;
    delete[] pS_size;
    delete[] prR_size;
    delete[] prS_size;
}


// For the mj+atomic/adaptive or the mj+greedy/adaptive setup.
void ParallelDomainBased_Partition_MiniJoinsBreakdown_Adaptive(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumBuckets, int runNumPartitionsPerRelation, int runNumThreads)
{
    int numGranules = ((runNumPartitionsPerRelation < 10)? runNumPartitionsPerRelation*1000: runNumPartitionsPerRelation*100);
    Timestamp gmin = std::min(R.minStart, S.minStart), gmax = std::max(R.maxEnd, S.maxEnd);
    auto granuleExtent = (Timestamp)ceil((double)(gmax-gmin)/numGranules);
    vector<size_t> shistR(numGranules, 0), shistS(numGranules, 0);
    vector<Timestamp> boundaries(runNumPartitionsPerRelation, 0);
    size_t *pR_size, *pS_size, *prR_size, *prS_size, *prfR_size, *prfS_size;
    
    
    // Initiliaze auxliary counter
    pR_size = new size_t[runNumPartitionsPerRelation];
    pS_size = new size_t[runNumPartitionsPerRelation];
    prR_size = new size_t[runNumPartitionsPerRelation];
    prS_size = new size_t[runNumPartitionsPerRelation];
    prfR_size = new size_t[runNumPartitionsPerRelation];
    prfS_size = new size_t[runNumPartitionsPerRelation];
    memset(pR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(pS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prfR_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    memset(prfS_size, 0, runNumPartitionsPerRelation*sizeof(size_t));
    
    // Partition relations.
    #pragma omp parallel num_threads(runNumThreads)
    {
        // Step 1: build histogram statistics for granules; i.e., count the records starting inside each granule
        // Use two parallel threads; one for each input relation.
        #pragma omp sections
        {
            #pragma omp section
            {
                BuildStartHistograms(R, shistR, gmax, granuleExtent, numGranules);
            }
            
            #pragma omp section
            {
                BuildStartHistograms(S, shistS, gmax, granuleExtent, numGranules);
            }
        }
        #pragma omp barrier
        
        
        // Step 2: reposition the boundaries between partitions; initial partitioning includes the same number of granules inside each partition.
        // Use one thread.
        #pragma omp single
        {
            AdaptivePartitioning(shistR, shistS, boundaries, gmax, granuleExtent, numGranules, runNumPartitionsPerRelation);
        }
        
        
        // Step 3: create and fill structures for each partition.
        // Use two parallel threads, one for each input relation.
        #pragma omp sections
        {
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(R, pR_size, prR_size, prfR_size, boundaries, runNumPartitionsPerRelation);
                
                // Allocate necessary memory.
                AllocateMemoryForPartitions(pR, prR, prfR, pR_size, prR_size, prfR_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                for (const auto& r : R)
                {
                    auto i = 0;
                    while ((i < runNumPartitionsPerRelation) && (boundaries[i] < r.start))
                        i++;
                    if (i == runNumPartitionsPerRelation)
                        i = runNumPartitionsPerRelation-1;
                    pR[i].push_back(r);
                    pR[i].minStart = std::min(pR[i].minStart, r.start);
                    pR[i].maxStart = std::max(pR[i].maxStart, r.start);
                    pR[i].minEnd   = std::min(pR[i].minEnd  , r.end);
                    pR[i].maxEnd   = std::max(pR[i].maxEnd  , r.end);

                    i++;
                    while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < r.end))
                    {
                        if ((boundaries[i] < r.end) && (i != runNumPartitionsPerRelation-1))
                            prfR[i].push_back(r);
                        else
                            prR[i].push_back(r);
                        i++;
                    }
                }
            }
            
            #pragma omp section
            {
                // Count first the contents of each partition.
                CountPartitionContents(S, pS_size, prS_size, prfS_size, boundaries, runNumPartitionsPerRelation);
                
                // Allocate necessary memory.
                AllocateMemoryForPartitions(pS, prS, prfS, pS_size, prS_size, prfS_size, runNumPartitionsPerRelation);
                
                // Fill structures on each partition.
                for (const auto& s : S)
                {
                    auto i = 0;
                    while ((i < runNumPartitionsPerRelation) && (boundaries[i] < s.start))
                        i++;
                    if (i == runNumPartitionsPerRelation)
                        i = runNumPartitionsPerRelation-1;
                    pS[i].push_back(s);
                    pS[i].minStart = std::min(pS[i].minStart, s.start);
                    pS[i].maxStart = std::max(pS[i].maxStart, s.start);
                    pS[i].minEnd   = std::min(pS[i].minEnd  , s.end);
                    pS[i].maxEnd   = std::max(pS[i].maxEnd  , s.end);

                    i++;
                    while ((i < runNumPartitionsPerRelation) && (boundaries[i-1] < s.end))
                    {
                        if ((boundaries[i] < s.end) && (i != runNumPartitionsPerRelation-1))
                            prfS[i].push_back(s);
                        else
                            prS[i].push_back(s);
                        i++;
                    }
                }
            }
        }
        #pragma omp barrier
        
        
        // Step 4: sort partitions; in practice we only to sort structures that contain exclusively original records, i.e., replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        sort(pR[i].begin(), pR[i].end());
                        break;
                        
                    case 1:
                        sort(pS[i].begin(), pS[i].end());
                        break;
                }
            }
        }
        #pragma omp barrier

        
        // Step 5: build bucket indices; in practice, we only to index original records, i.e., no replicas.
        // Use all available threads.
        #pragma omp for collapse(2) schedule(dynamic)
        for (int j = 0; j < 2; j++)
        {
            for (int i = 0; i < runNumPartitionsPerRelation; i++)
            {
                switch (j)
                {
                    case 0:
                        pBIR[i].build(pR[i], runNumBuckets);
                        break;
                        
                    case 1:
                        pBIS[i].build(pS[i], runNumBuckets);
                        break;
                }
            }
        }
    }
    
    // Free allocated memory.
    delete[] pR_size;
    delete[] pS_size;
    delete[] prR_size;
    delete[] prS_size;
    delete[] prfR_size;
    delete[] prfS_size;
}


void ParallelDomainBased_Partition(const Relation& R, const Relation& S, Relation *pR, Relation *pS, Relation *prR, Relation *prS, Relation *prfR, Relation *prfS, BucketIndex *pBIR, BucketIndex *pBIS, int runNumBuckets, int runNumPartitionsPerRelation, int runNumThreads, bool runMiniJoinsBreakdown, bool runAdaptivePartitioning)
{
    if (runMiniJoinsBreakdown)
    {
        if (runAdaptivePartitioning)
            ParallelDomainBased_Partition_MiniJoinsBreakdown_Adaptive(R, S, pR, pS, prR, prS, prfR, prfS, pBIR, pBIS, runNumBuckets, runNumPartitionsPerRelation, runNumThreads);
        else
            ParallelDomainBased_Partition_MiniJoinsBreakdown(R, S, pR, pS, prR, prS, prfR, prfS, pBIR, pBIS, runNumBuckets, runNumPartitionsPerRelation, runNumThreads);
    }
    else
    {
        if (runAdaptivePartitioning)
            ParallelDomainBased_Partition_Adaptive(R, S, pR, pS, prR, prS, pBIR, pBIS, runNumBuckets, runNumPartitionsPerRelation, runNumThreads);
        else
            ParallelDomainBased_Partition(R, S, pR, pS, prR, prS, pBIR, pBIS, runNumBuckets, runNumPartitionsPerRelation, runNumThreads);
    }
}
