#ifndef   UTIL_UTIL_FUNCTION_H_
#define   UTIL_UTIL_FUNCTION_H_

#include "config.h"

#include "string.h"
#include <math.h>

class UtilFunc
{

    /* 
     * 随机数种子
     * 每个线程拥有独立的 rand_buffer，根据线程ID映射一对一映射，线程间不存在竞争
     */
    static struct drand48_data rand_buffers_[g_max_client_num];

    /* 
     * TPCC负载有关的变量
     */
    /** NURand函数中的常量C **/
    static uint64_t C_C_LAST_255 , C_C_ID_1023, C_I_ID_8191;
    
    /** 包含 1-9 a-z A-Z 共61个字符，用于生成字符串 **/
    static const char  char_list_[61];
    /** 包含 10 个单词，用于生成customer的last name **/
    static const char* last_name_list_[10];


    static double denom;
    static double zeta_2_theta;
    static double eta;
    static double zipf2;


public:

    static void InitUtilFunc()
    {
        //初始化随机数种子
        for ( uint64_t i = 0; i < g_max_client_num; i++)
        {
            srand48_r(i+1, &UtilFunc::rand_buffers_[i]);
        }
        
        //初始化TPCC中NURand函数使用的常量C
        UtilFunc::C_C_LAST_255 = (uint64_t )UtilFunc::URand(0, 255, 0);
        UtilFunc::C_C_ID_1023  = (uint64_t )UtilFunc::URand(0, 1023, 0);
        UtilFunc::C_I_ID_8191  = (uint64_t )UtilFunc::URand(0, 8191, 0);

        UtilFunc::denom = UtilFunc::Zeta(g_ycsb_record_num - 1, g_ycsb_zipf_theta);
        UtilFunc::zeta_2_theta = UtilFunc::Zeta(2, g_ycsb_zipf_theta);
        UtilFunc::eta = (1 - pow(2.0 / g_ycsb_record_num, 1 - g_ycsb_zipf_theta))/
                    (1 - UtilFunc::zeta_2_theta / UtilFunc::denom);
        UtilFunc::zipf2 = pow(0.5, g_ycsb_zipf_theta);

    }
    
    /** 在区间 [0, max - 1] 范围内生成平均分布的整型变量 **/
    static uint64_t Rand(uint64_t  max, uint64_t thread_id)
    {
        int64_t rint64 = 0;
	    lrand48_r(&UtilFunc::rand_buffers_[thread_id], &rint64);
	    return rint64 % max;
    }


    /** 在区间 [0, 1] 范围内生成平均分布的double变量 **/
    static double DRand(uint64_t thread_id)
    {
        double drand = 0;
	    drand48_r(&UtilFunc::rand_buffers_[thread_id], &drand);
	    return drand;
    }


    /** 在区间[min, max]中生成平均分布的整型变量 **/
    static uint64_t URand(uint64_t  min, uint64_t max, uint64_t thread_id)
    {
        return min + UtilFunc::Rand(max - min + 1, thread_id);
    }

    /*
     * tpcc负载中使用的NURand函数
     * @param A: 255(C_LAST) 1023(C_ID) 8191(OL_I_ID)
     */
    static uint64_t NURand(uint64_t  A, uint64_t x, uint64_t y, uint64_t thread_id)
    {
        uint64_t C = 0;
        switch(A) {
            case 255:
                C = C_C_LAST_255;
                break;
            case 1023:
                C = C_C_ID_1023;
                break;
            case 8191:
                C = C_I_ID_8191;
                break;
            default:
                printf("NURand函数接收到无法识别的随机常数!\n");
                break;
        }
        return(((UtilFunc::URand(0, A, thread_id) | UtilFunc::URand(x, y, thread_id)) + C) % (y - x + 1)) + x;
    }

    /*
     * 生成长度在[min, max]之间的字符串，字符串可包括数字1-9、大小写字母
     * 注意，传入的str必须已经申请足够的空间，函数只负责填充，不负责申请空间
     */
    static uint64_t MakeAlphaString(uint64_t min, uint64_t max, char* str, uint64_t thread_id)
    {
        uint64_t length = UtilFunc::URand(min, max, thread_id);
        for (uint32_t i = 0; i < length; i++) 
            str[i] = char_list_[UtilFunc::URand(0L, 60L, thread_id)];
        for (uint64_t i = length; i < max; i++)
            str[i] = '\0';

        return length;
    }

    /*
     * 同上，只是生成的字符串只包括数字0-9
     */
    static uint64_t MakeNumberString(uint64_t min, uint64_t max, char* str, uint64_t thread_id)
    {
        uint64_t length = UtilFunc::URand(min, max, thread_id);
        for (uint32_t i = 0; i < length; i++) {
            uint64_t r = UtilFunc::URand(0L,9L, thread_id);
            str[i] = '0' + r;
        }
        return length;
    }

    /** 生成customer的last name
     *  传入0-999范围内的整数 **/
    static uint64_t Lastname(uint64_t  num, char* last_name)
    {
        strcpy(last_name, last_name_list_[num/100]);     //百位
        strcat(last_name, last_name_list_[(num/10)%10]); //十位
        strcat(last_name, last_name_list_[num%10]);      //个位
        return strlen(last_name);
    }

    static double Zeta(uint64_t n, double theta)
    {
        uint64_t i;
        double ans = 0;
        for (i = 1; i <=n; i++)
        {
            ans += 1.0 / pow(i, theta);
        }
        return ans;
    }

	static uint64_t Zipf(uint64_t n, double theta, ClientID client_id)
    {
        double alpha  = 1.0 / (1.0 - theta);
        double zetan  = UtilFunc::denom;
        // double u      = double(UtilFunc::Rand(10000000000, client_id)/10000000000.0);
        double u      = UtilFunc::DRand(client_id);
        
        double uz     = u * zetan;
        if(uz < 1) return 1;
        if(uz < 1 + UtilFunc::zipf2) return 2;
        return 1 + (uint64_t)(n * pow(UtilFunc::eta * u - UtilFunc::eta + 1, alpha));
    }

    // 冒泡排序
    static void Sort(uint64_t array[], uint64_t start_index, uint64_t end_index)
    {
        for (int i = start_index; i <= end_index; i++) {
            // 提前退出优化：如果某一轮没有发生交换，说明已经有序
            bool swapped = false;
            for (int j = start_index; j <= end_index - i; j++) {
                if (array[j] > array[j + 1]) {
                    // 交换相邻元素
                    swap(array[j], array[j + 1]);
                    swapped = true;
                }
            }
            if (!swapped) break;  // 提前结束排序
        }
    }

};



#endif