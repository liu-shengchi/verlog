#ifndef BENCHMARK_YCSB_UTIL_H_
#define BENCHMARK_YCSB_UTIL_H_

#include "config.h"

#include "ycsb_config.h"

#include "util_function.h"

#include <math.h>


#if   WORKLOAD_TYPE == YCSB_W

/*********** YCSB Util Function ***********/

class YCSBUtilFunc
{
private: 

    static double denom;
    static double zeta_2_theta;
    static double eta;
    static double zipf2;

public:

    static void InitYCSBUtilFunc()
    {
        YCSBUtilFunc::denom = YCSBUtilFunc::Zeta(g_ycsb_record_num_per_shard - 1, g_ycsb_zipf_theta);
        YCSBUtilFunc::zeta_2_theta = YCSBUtilFunc::Zeta(2, g_ycsb_zipf_theta);
        YCSBUtilFunc::eta = (1 - pow(2.0 / g_ycsb_record_num_per_shard, 1 - g_ycsb_zipf_theta))/
                    (1 - YCSBUtilFunc::zeta_2_theta / YCSBUtilFunc::denom);
        YCSBUtilFunc::zipf2 = pow(0.5, g_ycsb_zipf_theta);
    }

    static double Zeta(uint64_t n, double theta)
    {
        double ans = 0;
        for (uint64_t i = 1; i <=n; i++)
        {
            ans += 1.0 / pow(i, theta);
        }
        return ans;
    }

	static uint64_t Zipf(uint64_t n, double theta, ThreadID thread_id)
    {
        double alpha  = 1.0 / (1.0 - theta);
        double zetan  = YCSBUtilFunc::denom;
        // double u      = double(UtilFunc::Rand(10000000000, thread_id))/10000000000.0;
        double u      = UtilFunc::DRand(thread_id);
        double uz     = u * zetan;
        if(uz < 1) return 1;
        if(uz < 1 + YCSBUtilFunc::zipf2) return 2;
        return 1 + (uint64_t)(n * pow(YCSBUtilFunc::eta * u - YCSBUtilFunc::eta + 1, alpha));
    }

};



#endif
#endif