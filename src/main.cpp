/*=============================================================================
#  Author:           blu (bruce.lu)
#  Email:            lzbgt@126.com 
#  FileName:         main.cpp
#  Description:      /
#  Version:          0.0.1
#  History:         
=============================================================================*/
#include <thread>
#include "inc/ezviz.hpp"

int main() {
    //
    EZVizVideoService svc = EZVizVideoService();
    svc.Run();
    this_thread::sleep_for(20s);

    return 0;
}