#include "util_function.h"

struct drand48_data UtilFunc::rand_buffers_[g_max_thread_num];


const char UtilFunc::char_list_[61] = {'1','2','3','4','5','6','7','8','9','a','b','c',
                        'd','e','f','g','h','i','j','k','l','m','n','o',
                        'p','q','r','s','t','u','v','w','x','y','z','A',
                        'B','C','D','E','F','G','H','I','J','K','L','M',
                        'N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};