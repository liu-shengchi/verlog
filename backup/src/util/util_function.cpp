#include "util_function.h"

struct drand48_data UtilFunc::rand_buffers_[g_max_client_num];

uint64_t UtilFunc::C_C_LAST_255 = 0; 
uint64_t UtilFunc::C_C_ID_1023  = 0; 
uint64_t UtilFunc::C_I_ID_8191  = 0;

const char UtilFunc::char_list_[61] = {'1','2','3','4','5','6','7','8','9','a','b','c',
                        'd','e','f','g','h','i','j','k','l','m','n','o',
                        'p','q','r','s','t','u','v','w','x','y','z','A',
                        'B','C','D','E','F','G','H','I','J','K','L','M',
                        'N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};

const char* UtilFunc::last_name_list_[10] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
                                "ESE", "ANTI", "CALLY", "ATION", "EING"};


double UtilFunc::denom        = 0.0;
double UtilFunc::zeta_2_theta = 0.0;
double UtilFunc::eta          = 0.0;
double UtilFunc::zipf2        = 0.0;
