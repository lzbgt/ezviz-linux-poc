#include <ESOpenStream.h>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include "json.hpp"
#include <cstring>

using json = nlohmann::json;

#define TOKEN "at.7g1oe8qb6soxp5okazs8izk77ktm57yf-64bok5pgco-1wrhg87-tftmsjyow"
#define PRTRET std::cout << "return value: " << ret<< std::endl;

std::ofstream fout;

int msgCb(HANDLE pHandle, int code, int eventType, void *pUser) {
    std::cout << "=====> msg h: " << pHandle << " code: " << code << " evt: " << eventType << " pd: " << pUser<< std::endl;
    if(code == ES_STREAM_CODE::ES_STREAM_CLIENT_RET_OVER) {
        (*(std::ofstream *)pUser).close();
    }
    return 0;
}

int dataCb(HANDLE pHandle, unsigned int dataType, unsigned char *buf,  unsigned int buflen, void* pUser){
    std::cout << "=====> data h: " << pHandle << " dataType: " << dataType << " len: " << buflen << " pd: " << pUser<< std::endl;
    (*(std::ofstream *)pUser).write(reinterpret_cast<const char*>(buf), buflen);
    return 0;
}
int main(){
    int ret = 0;
    // init
    ESOpenSDK_Init(2,1);
    ESOpenSDK_InitWithAppKey("a287e05ace374c3587e051db8cd4be82", "https://open.ys7.com" );
    PRTRET

    // dev info
    ST_ES_DEVICE_INFO dev = {"C90843484", 1, "VGAJRZ"};
    void * pOut = NULL;
    int length = 0;
    ret = ESOpenSDK_GetDevInfo(TOKEN, dev, false, &pOut, &length);
    PRTRET
    std::cout << (char *)pOut << std::endl;
    ESOpenSDK_FreeData(pOut);
    pOut=NULL;

    // video level
    ret = ESOpenSDK_SetVideoLevel(TOKEN, dev, 3);
    PRTRET
    // records list
    ES_RECORD_INFO ri = {"2019-05-28 17:00:00", "2019-05-28 23:00:19", 0, ""};
    ret = ESOpenSDK_SearchVideoRecord(TOKEN, dev, ri , &pOut, &length);
    PRTRET
    std::cout << "==== JSON:\n";
    std::cout << (char *)pOut << std::endl;
    json j = json::parse((char*)pOut);
    ESOpenSDK_FreeData(pOut);
    pOut=NULL;

    for(auto &i : j["data"]) {
        if(i["fileSize"] != 0) {
            std::cout<< "==== pick the first non-empty file to download:";
            std::cout<<std::endl<< i.dump()<<std::endl;
            HANDLE handle = NULL;
            fout.open("a.mpg",std::ios::binary);
            ES_STREAM_CALLBACK scb = {msgCb, dataCb, (void*)&fout};
            strcpy(ri.szStartTime, i["beginTime"].get<std::string>().c_str());
            strcpy(ri.szStopTime, i["endTime"].get<std::string>().c_str());
            ri.iRecType = i["recType"];
            ret = ESOpenSDK_StartPlayBack(TOKEN, dev, ri, scb, handle);
            PRTRET
            break;
        }
    }

    

    // // real play
    // ret = ESOpenSDK_StartRealPlay(TOKEN, dev, scb, handle);
    usleep(100 * 1000 * 1000);
    ESOpenSDK_Fini();
    
}