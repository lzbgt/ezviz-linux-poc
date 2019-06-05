/*=============================================================================
#  Author:           blu (bruce.lu)
#  Email:            lzbgt@icloud.com 
#  FileName:         main.cpp
#  Description:      /
#  Version:          0.0.1
#  History:         
=============================================================================*/
#include <thread>
#include "inc/ezviz.hpp"

int main() {
    //
    EZVizVideoService svc;
    while(true) {
        // reconnect network
        svc.Run();
    }

    return 0;
}