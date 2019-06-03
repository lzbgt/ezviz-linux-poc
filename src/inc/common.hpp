#ifndef __MY_COMMON_H__
#define __MY_COMMON_H__

#include <string>
#include <mutex>
#include <fstream>
using namespace std;

typedef struct EZAMPQConfig {
    string exchangeName;
    string queName;
    string routeKey;
} EZAMPQConfig;

typedef struct EnvConfig {
    string appKey;
    string appSecret;
    string videoDir;
    string amqpAddr;
    EZAMPQConfig amqpPlayBack;
    EZAMPQConfig amqpRTRecord;
    string azureStorageName;
    string azureStorageKey;
    string azureEndpoint;
    int ezvizNumTcpThreadsMax;
    int ezvizNumSslThreadsMax;
}EnvConfig;

typedef struct EZCallBackUserData{
    // video file handler
    ofstream *fout;
    // 0: download ended; otherwise: downloading
    int stat;
    // guard multi-thread file writing
    mutex m;
    // retried times on network failure etc. TODO: NOT IMPLEMENTED
    int numRetried;
}EZCallBackUserData;

#endif