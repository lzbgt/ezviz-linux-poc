#ifndef __MY_UUID_H__
#define __MY_UUID_H__

#include <string>
#include <cstdlib>

//*Adapted from https://gist.github.com/ne-sachirou/882192
//*std::rand() can be replaced with other algorithms as Xorshift for better perfomance
//*Random seed must be initialized by user

namespace myutils {
    const std::string CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::string GenerateUUID(char sep = '-'){

    std::string uuid = std::string(36,' ');
    std::srand(time(NULL));
    int rnd = std::rand();
    int r = 0;
    
    uuid[8] = sep;
    uuid[13] = sep;
    uuid[18] = sep;
    uuid[23] = sep;

    uuid[14] = '4';

    for(int i=0;i<36;i++){
        if (i != 8 && i != 13 && i != 18 && i != 14 && i != 23) {
        if (rnd <= 0x02) {
            rnd = 0x2000000 + (std::rand() * 0x1000000) | 0;
        }
        rnd >>= 4;
        uuid[i] = CHARS[(i == 19) ? ((rnd & 0xf) & 0x3) | 0x8 : rnd & 0xf];
        }
    }
    return uuid;
    }
};

#endif