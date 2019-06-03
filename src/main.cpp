/*=============================================================================
#  Author:           blu (bruce.lu)
#  Email:            lzbgt@126.com 
#  FileName:         main.cpp
#  Description:      /
#  Version:          0.0.1
#  History:         
=============================================================================*/

#include "inc/ezviz.hpp"

int main() {
    // get env
    EnvConfig envConfig = {};
    EZVizVideoService svc = EZVizVideoService(envConfig);
    svc.Run();

    return 0;
}