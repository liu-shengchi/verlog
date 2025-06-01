#ifndef BENCHMARK_TPCC_UTIL_H_
#define BENCHMARK_TPCC_UTIL_H_

#include "config.h"

#include "util_function.h"

#include "string.h"


#if   WORKLOAD_TYPE == TPCC_W


/*********** TPCC Util Function ***********/

class TPCCUtilFunc
{
private: 

    /** NURand函数中的常量C **/
    static uint64_t C_C_LAST_255 , C_C_ID_1023, C_I_ID_8191;
    
    /** 包含 1-9 a-z A-Z 共61个字符，用于生成字符串 **/
    static const char  char_list_[61];
    /** 包含 10 个单词，用于生成customer的last name **/
    static const char* last_name_list_[10];


public:

    static void InitTPCCUtilFunc()
    {
        
        //初始化TPCC中NURand函数使用的常量C
        TPCCUtilFunc::C_C_LAST_255 = (uint64_t )UtilFunc::URand(0, 255, 0);
        TPCCUtilFunc::C_C_ID_1023  = (uint64_t )UtilFunc::URand(0, 1023, 0);
        TPCCUtilFunc::C_I_ID_8191  = (uint64_t )UtilFunc::URand(0, 8191, 0);
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

};



#endif
#endif