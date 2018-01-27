#include "../def.h"
#include "../containers/relation.h"



unsigned long long NestedLoops_Rolled(const Relation &R, const Relation &S)
{
    unsigned long long result = 0;
    auto lastR = R.end();
    auto lastS = S.end();
    
    
    for (auto r = R.begin(); r != lastR; r++)
    {
        for (auto s = S.begin(); s != lastS; s++)
        {
            result += r->start ^ s->start;
        }
    }
    
    
    return result;
}


unsigned long long NestedLoops_Unrolled(const Relation &R, const Relation &S)
{
    unsigned long long result = 0;
    auto lastR = R.end();
    auto lastS = S.end();
    auto bufferSize = R.size();
    auto pivot = S.begin();
    
    
    for (auto curr = R.begin(); curr != lastR; curr++)
    {
        switch (bufferSize)
        {
            case 1:
                while (lastS-pivot >= 32)
                {
                    result += curr->start ^ (pivot+0)->start;
                    result += curr->start ^ (pivot+1)->start;
                    result += curr->start ^ (pivot+2)->start;
                    result += curr->start ^ (pivot+3)->start;
                    result += curr->start ^ (pivot+4)->start;
                    result += curr->start ^ (pivot+5)->start;
                    result += curr->start ^ (pivot+6)->start;
                    result += curr->start ^ (pivot+7)->start;
                    result += curr->start ^ (pivot+8)->start;
                    result += curr->start ^ (pivot+9)->start;
                    result += curr->start ^ (pivot+10)->start;
                    result += curr->start ^ (pivot+11)->start;
                    result += curr->start ^ (pivot+12)->start;
                    result += curr->start ^ (pivot+13)->start;
                    result += curr->start ^ (pivot+14)->start;
                    result += curr->start ^ (pivot+15)->start;
                    result += curr->start ^ (pivot+16)->start;
                    result += curr->start ^ (pivot+17)->start;
                    result += curr->start ^ (pivot+18)->start;
                    result += curr->start ^ (pivot+19)->start;
                    result += curr->start ^ (pivot+20)->start;
                    result += curr->start ^ (pivot+21)->start;
                    result += curr->start ^ (pivot+22)->start;
                    result += curr->start ^ (pivot+23)->start;
                    result += curr->start ^ (pivot+24)->start;
                    result += curr->start ^ (pivot+25)->start;
                    result += curr->start ^ (pivot+26)->start;
                    result += curr->start ^ (pivot+27)->start;
                    result += curr->start ^ (pivot+28)->start;
                    result += curr->start ^ (pivot+29)->start;
                    result += curr->start ^ (pivot+30)->start;
                    result += curr->start ^ (pivot+31)->start;
                    pivot += 32;
                }
                
                while (pivot < lastS)
                {
                    result += curr->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 2:
                while (lastS-pivot >= 16)
                {
                    result += (curr+0)->start ^ (pivot+0)->start;
                    result += (curr+1)->start ^ (pivot+0)->start;
                    result += (curr+0)->start ^ (pivot+1)->start;
                    result += (curr+1)->start ^ (pivot+1)->start;
                    result += (curr+0)->start ^ (pivot+2)->start;
                    result += (curr+1)->start ^ (pivot+2)->start;
                    result += (curr+0)->start ^ (pivot+3)->start;
                    result += (curr+1)->start ^ (pivot+3)->start;
                    result += (curr+0)->start ^ (pivot+4)->start;
                    result += (curr+1)->start ^ (pivot+4)->start;
                    result += (curr+0)->start ^ (pivot+5)->start;
                    result += (curr+1)->start ^ (pivot+5)->start;
                    result += (curr+0)->start ^ (pivot+6)->start;
                    result += (curr+1)->start ^ (pivot+6)->start;
                    result += (curr+0)->start ^ (pivot+7)->start;
                    result += (curr+1)->start ^ (pivot+7)->start;
                    result += (curr+0)->start ^ (pivot+8)->start;
                    result += (curr+1)->start ^ (pivot+8)->start;
                    result += (curr+0)->start ^ (pivot+9)->start;
                    result += (curr+1)->start ^ (pivot+9)->start;
                    result += (curr+0)->start ^ (pivot+10)->start;
                    result += (curr+1)->start ^ (pivot+10)->start;
                    result += (curr+0)->start ^ (pivot+11)->start;
                    result += (curr+1)->start ^ (pivot+11)->start;
                    result += (curr+0)->start ^ (pivot+12)->start;
                    result += (curr+1)->start ^ (pivot+12)->start;
                    result += (curr+0)->start ^ (pivot+13)->start;
                    result += (curr+1)->start ^ (pivot+13)->start;
                    result += (curr+0)->start ^ (pivot+14)->start;
                    result += (curr+1)->start ^ (pivot+14)->start;
                    result += (curr+0)->start ^ (pivot+15)->start;
                    result += (curr+1)->start ^ (pivot+15)->start;
                    pivot += 16;
                }
                
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 3:
                while (lastS-pivot >= 8)
                {
                    result += (curr+0)->start ^ (pivot+0)->start;
                    result += (curr+1)->start ^ (pivot+0)->start;
                    result += (curr+2)->start ^ (pivot+0)->start;
                    result += (curr+0)->start ^ (pivot+1)->start;
                    result += (curr+1)->start ^ (pivot+1)->start;
                    result += (curr+2)->start ^ (pivot+1)->start;
                    result += (curr+0)->start ^ (pivot+2)->start;
                    result += (curr+1)->start ^ (pivot+2)->start;
                    result += (curr+2)->start ^ (pivot+2)->start;
                    result += (curr+0)->start ^ (pivot+3)->start;
                    result += (curr+1)->start ^ (pivot+3)->start;
                    result += (curr+2)->start ^ (pivot+3)->start;
                    result += (curr+0)->start ^ (pivot+4)->start;
                    result += (curr+1)->start ^ (pivot+4)->start;
                    result += (curr+2)->start ^ (pivot+4)->start;
                    result += (curr+0)->start ^ (pivot+5)->start;
                    result += (curr+1)->start ^ (pivot+5)->start;
                    result += (curr+2)->start ^ (pivot+5)->start;
                    result += (curr+0)->start ^ (pivot+6)->start;
                    result += (curr+1)->start ^ (pivot+6)->start;
                    result += (curr+2)->start ^ (pivot+6)->start;
                    result += (curr+0)->start ^ (pivot+7)->start;
                    result += (curr+1)->start ^ (pivot+7)->start;
                    result += (curr+2)->start ^ (pivot+7)->start;
                    pivot += 8;
                }
                
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 4:
                while (lastS-pivot >= 8)
                {
                    result += (curr+0)->start ^ (pivot+0)->start;
                    result += (curr+1)->start ^ (pivot+0)->start;
                    result += (curr+2)->start ^ (pivot+0)->start;
                    result += (curr+3)->start ^ (pivot+0)->start;
                    result += (curr+0)->start ^ (pivot+1)->start;
                    result += (curr+1)->start ^ (pivot+1)->start;
                    result += (curr+2)->start ^ (pivot+1)->start;
                    result += (curr+3)->start ^ (pivot+1)->start;
                    result += (curr+0)->start ^ (pivot+2)->start;
                    result += (curr+1)->start ^ (pivot+2)->start;
                    result += (curr+2)->start ^ (pivot+2)->start;
                    result += (curr+3)->start ^ (pivot+2)->start;
                    result += (curr+0)->start ^ (pivot+3)->start;
                    result += (curr+1)->start ^ (pivot+3)->start;
                    result += (curr+2)->start ^ (pivot+3)->start;
                    result += (curr+3)->start ^ (pivot+3)->start;
                    result += (curr+0)->start ^ (pivot+4)->start;
                    result += (curr+1)->start ^ (pivot+4)->start;
                    result += (curr+2)->start ^ (pivot+4)->start;
                    result += (curr+3)->start ^ (pivot+4)->start;
                    result += (curr+0)->start ^ (pivot+5)->start;
                    result += (curr+1)->start ^ (pivot+5)->start;
                    result += (curr+2)->start ^ (pivot+5)->start;
                    result += (curr+3)->start ^ (pivot+5)->start;
                    result += (curr+0)->start ^ (pivot+6)->start;
                    result += (curr+1)->start ^ (pivot+6)->start;
                    result += (curr+2)->start ^ (pivot+6)->start;
                    result += (curr+3)->start ^ (pivot+6)->start;
                    result += (curr+0)->start ^ (pivot+7)->start;
                    result += (curr+1)->start ^ (pivot+7)->start;
                    result += (curr+2)->start ^ (pivot+7)->start;
                    result += (curr+3)->start ^ (pivot+7)->start;
                    pivot += 8;
                }
                
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 5:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 6:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 7:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 8:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 9:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 10:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 11:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 12:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 13:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 14:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 15:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 16:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 17:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 18:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 19:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 20:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 21:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 22:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 23:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    result += (curr+22)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 24:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    result += (curr+22)->start ^ pivot->start;
                    result += (curr+23)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 25:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    result += (curr+22)->start ^ pivot->start;
                    result += (curr+23)->start ^ pivot->start;
                    result += (curr+24)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 26:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    result += (curr+22)->start ^ pivot->start;
                    result += (curr+23)->start ^ pivot->start;
                    result += (curr+24)->start ^ pivot->start;
                    result += (curr+25)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 27:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    result += (curr+22)->start ^ pivot->start;
                    result += (curr+23)->start ^ pivot->start;
                    result += (curr+24)->start ^ pivot->start;
                    result += (curr+25)->start ^ pivot->start;
                    result += (curr+26)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 28:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    result += (curr+22)->start ^ pivot->start;
                    result += (curr+23)->start ^ pivot->start;
                    result += (curr+24)->start ^ pivot->start;
                    result += (curr+25)->start ^ pivot->start;
                    result += (curr+26)->start ^ pivot->start;
                    result += (curr+27)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 29:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    result += (curr+22)->start ^ pivot->start;
                    result += (curr+23)->start ^ pivot->start;
                    result += (curr+24)->start ^ pivot->start;
                    result += (curr+25)->start ^ pivot->start;
                    result += (curr+26)->start ^ pivot->start;
                    result += (curr+27)->start ^ pivot->start;
                    result += (curr+28)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 30:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    result += (curr+22)->start ^ pivot->start;
                    result += (curr+23)->start ^ pivot->start;
                    result += (curr+24)->start ^ pivot->start;
                    result += (curr+25)->start ^ pivot->start;
                    result += (curr+26)->start ^ pivot->start;
                    result += (curr+27)->start ^ pivot->start;
                    result += (curr+28)->start ^ pivot->start;
                    result += (curr+29)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 31:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    result += (curr+22)->start ^ pivot->start;
                    result += (curr+23)->start ^ pivot->start;
                    result += (curr+24)->start ^ pivot->start;
                    result += (curr+25)->start ^ pivot->start;
                    result += (curr+26)->start ^ pivot->start;
                    result += (curr+27)->start ^ pivot->start;
                    result += (curr+28)->start ^ pivot->start;
                    result += (curr+29)->start ^ pivot->start;
                    result += (curr+30)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            case 32:
                while (pivot < lastS)
                {
                    result += (curr+0)->start ^ pivot->start;
                    result += (curr+1)->start ^ pivot->start;
                    result += (curr+2)->start ^ pivot->start;
                    result += (curr+3)->start ^ pivot->start;
                    result += (curr+4)->start ^ pivot->start;
                    result += (curr+5)->start ^ pivot->start;
                    result += (curr+6)->start ^ pivot->start;
                    result += (curr+7)->start ^ pivot->start;
                    result += (curr+8)->start ^ pivot->start;
                    result += (curr+9)->start ^ pivot->start;
                    result += (curr+10)->start ^ pivot->start;
                    result += (curr+11)->start ^ pivot->start;
                    result += (curr+12)->start ^ pivot->start;
                    result += (curr+13)->start ^ pivot->start;
                    result += (curr+14)->start ^ pivot->start;
                    result += (curr+15)->start ^ pivot->start;
                    result += (curr+16)->start ^ pivot->start;
                    result += (curr+17)->start ^ pivot->start;
                    result += (curr+18)->start ^ pivot->start;
                    result += (curr+19)->start ^ pivot->start;
                    result += (curr+20)->start ^ pivot->start;
                    result += (curr+21)->start ^ pivot->start;
                    result += (curr+22)->start ^ pivot->start;
                    result += (curr+23)->start ^ pivot->start;
                    result += (curr+24)->start ^ pivot->start;
                    result += (curr+25)->start ^ pivot->start;
                    result += (curr+26)->start ^ pivot->start;
                    result += (curr+27)->start ^ pivot->start;
                    result += (curr+28)->start ^ pivot->start;
                    result += (curr+29)->start ^ pivot->start;
                    result += (curr+30)->start ^ pivot->start;
                    result += (curr+31)->start ^ pivot->start;
                    pivot++;
                }
                break;
                
            default:
                while (lastS-pivot >= 32)
                {
                    for (auto k = curr; k != lastR; k++)
                    {
                        result += k->start ^ (pivot+0)->start;
                        result += k->start ^ (pivot+1)->start;
                        result += k->start ^ (pivot+2)->start;
                        result += k->start ^ (pivot+3)->start;
                        result += k->start ^ (pivot+4)->start;
                        result += k->start ^ (pivot+5)->start;
                        result += k->start ^ (pivot+6)->start;
                        result += k->start ^ (pivot+7)->start;
                        result += k->start ^ (pivot+8)->start;
                        result += k->start ^ (pivot+9)->start;
                        result += k->start ^ (pivot+10)->start;
                        result += k->start ^ (pivot+11)->start;
                        result += k->start ^ (pivot+12)->start;
                        result += k->start ^ (pivot+13)->start;
                        result += k->start ^ (pivot+14)->start;
                        result += k->start ^ (pivot+15)->start;
                        result += k->start ^ (pivot+16)->start;
                        result += k->start ^ (pivot+17)->start;
                        result += k->start ^ (pivot+18)->start;
                        result += k->start ^ (pivot+19)->start;
                        result += k->start ^ (pivot+20)->start;
                        result += k->start ^ (pivot+21)->start;
                        result += k->start ^ (pivot+22)->start;
                        result += k->start ^ (pivot+23)->start;
                        result += k->start ^ (pivot+24)->start;
                        result += k->start ^ (pivot+25)->start;
                        result += k->start ^ (pivot+26)->start;
                        result += k->start ^ (pivot+27)->start;
                        result += k->start ^ (pivot+28)->start;
                        result += k->start ^ (pivot+29)->start;
                        result += k->start ^ (pivot+30)->start;
                        result += k->start ^ (pivot+31)->start;
                    }
                    pivot += 32;
                }
                
                if (lastS-pivot >= 16)
                {
                    for (auto k = curr; k != lastR; k++)
                    {
                        result += k->start ^ (pivot+0)->start;
                        result += k->start ^ (pivot+1)->start;
                        result += k->start ^ (pivot+2)->start;
                        result += k->start ^ (pivot+3)->start;
                        result += k->start ^ (pivot+4)->start;
                        result += k->start ^ (pivot+5)->start;
                        result += k->start ^ (pivot+6)->start;
                        result += k->start ^ (pivot+7)->start;
                        result += k->start ^ (pivot+8)->start;
                        result += k->start ^ (pivot+9)->start;
                        result += k->start ^ (pivot+10)->start;
                        result += k->start ^ (pivot+11)->start;
                        result += k->start ^ (pivot+12)->start;
                        result += k->start ^ (pivot+13)->start;
                        result += k->start ^ (pivot+14)->start;
                        result += k->start ^ (pivot+15)->start;
                    }
                    pivot += 16;
                }
                
                if (lastS-pivot >= 8)
                {
                    for (auto k = curr; k != lastR; k++)
                    {
                        result += k->start ^ (pivot+0)->start;
                        result += k->start ^ (pivot+1)->start;
                        result += k->start ^ (pivot+2)->start;
                        result += k->start ^ (pivot+3)->start;
                        result += k->start ^ (pivot+4)->start;
                        result += k->start ^ (pivot+5)->start;
                        result += k->start ^ (pivot+6)->start;
                        result += k->start ^ (pivot+7)->start;
                    }
                    pivot += 8;
                }
                
                if (lastS-pivot >= 4)
                {
                    for (auto k = curr; k != lastR; k++)
                    {
                        result += k->start ^ (pivot+0)->start;
                        result += k->start ^ (pivot+1)->start;
                        result += k->start ^ (pivot+2)->start;
                        result += k->start ^ (pivot+3)->start;
                    }
                    pivot += 4;
                }
                
                while (pivot < lastS)
                {
                    auto k = curr;
                    while (lastR-k >= 32)
                    {
                        result += (k+0)->start ^ pivot->start;
                        result += (k+1)->start ^ pivot->start;
                        result += (k+2)->start ^ pivot->start;
                        result += (k+3)->start ^ pivot->start;
                        result += (k+4)->start ^ pivot->start;
                        result += (k+5)->start ^ pivot->start;
                        result += (k+6)->start ^ pivot->start;
                        result += (k+7)->start ^ pivot->start;
                        result += (k+8)->start ^ pivot->start;
                        result += (k+9)->start ^ pivot->start;
                        result += (k+10)->start ^ pivot->start;
                        result += (k+11)->start ^ pivot->start;
                        result += (k+12)->start ^ pivot->start;
                        result += (k+13)->start ^ pivot->start;
                        result += (k+14)->start ^ pivot->start;
                        result += (k+15)->start ^ pivot->start;
                        result += (k+16)->start ^ pivot->start;
                        result += (k+17)->start ^ pivot->start;
                        result += (k+18)->start ^ pivot->start;
                        result += (k+19)->start ^ pivot->start;
                        result += (k+20)->start ^ pivot->start;
                        result += (k+21)->start ^ pivot->start;
                        result += (k+22)->start ^ pivot->start;
                        result += (k+23)->start ^ pivot->start;
                        result += (k+24)->start ^ pivot->start;
                        result += (k+25)->start ^ pivot->start;
                        result += (k+26)->start ^ pivot->start;
                        result += (k+27)->start ^ pivot->start;
                        result += (k+28)->start ^ pivot->start;
                        result += (k+29)->start ^ pivot->start;
                        result += (k+30)->start ^ pivot->start;
                        result += (k+31)->start ^ pivot->start;
                        k += 32;
                    }
                    
                    if (lastR-k >= 16)
                    {
                        result += (k+0)->start ^ pivot->start;
                        result += (k+1)->start ^ pivot->start;
                        result += (k+2)->start ^ pivot->start;
                        result += (k+3)->start ^ pivot->start;
                        result += (k+4)->start ^ pivot->start;
                        result += (k+5)->start ^ pivot->start;
                        result += (k+6)->start ^ pivot->start;
                        result += (k+7)->start ^ pivot->start;
                        result += (k+8)->start ^ pivot->start;
                        result += (k+9)->start ^ pivot->start;
                        result += (k+10)->start ^ pivot->start;
                        result += (k+11)->start ^ pivot->start;
                        result += (k+12)->start ^ pivot->start;
                        result += (k+13)->start ^ pivot->start;
                        result += (k+14)->start ^ pivot->start;
                        result += (k+15)->start ^ pivot->start;
                        k += 16;
                    }
                    
                    if (lastR-k >= 8)
                    {
                        result += (k+0)->start ^ pivot->start;
                        result += (k+1)->start ^ pivot->start;
                        result += (k+2)->start ^ pivot->start;
                        result += (k+3)->start ^ pivot->start;
                        result += (k+4)->start ^ pivot->start;
                        result += (k+5)->start ^ pivot->start;
                        result += (k+6)->start ^ pivot->start;
                        result += (k+7)->start ^ pivot->start;
                        k += 8;
                    }
                    
                    if (lastR-k >= 4)
                    {
                        result += (k+0)->start ^ pivot->start;
                        result += (k+1)->start ^ pivot->start;
                        result += (k+2)->start ^ pivot->start;
                        result += (k+3)->start ^ pivot->start;
                        k += 4;
                    }
                    
                    while (k != lastR)
                    {
                        result += k->start ^ pivot->start; k++;
                    }
                    
                    pivot++;
                }
                break;
        }
    }
    
    
    return result;
}
