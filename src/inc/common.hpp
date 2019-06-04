#ifndef __MY_COMMON_H__
#define __MY_COMMON_H__

#include <string>
#include <mutex>
#include <fstream>
#include <iostream>
using namespace std;

typedef struct EZAMPQConfig {
    string amqpAddr;
    string exchangeName;
    string queName;
    string routeKey;
} EZAMPQConfig;

typedef struct EnvConfig {
    public:
    string mode; /* EZ_MODE: playback, rtplay */
    string appKey; /* EZ_APPKEY:  */
    string appSecret; /* EZ_APPSECRET */
    string videoDir; /* EZ_VIDEO_DIR */
    EZAMPQConfig amqpConfig; /* EZ_AMQP_ADDR, EZ_AMQP_EXCH, EZ_AMQP_QUEUE, EZ_AMQP_ROUTE */
    int ezvizNumTcpThreadsMax; /* EZ_NUM_TCPTHREADS */
    int ezvizNumSslThreadsMax; /* EZ_NUM_SSLTHREADS */
    EnvConfig(){
        //
    }
    EnvConfig(int whatever){
        cout << "geting environment variables" << endl;
        char *envStr;
        if(envStr = getenv("EZ_MODE")){
            this->mode = string(envStr);
        }
        if(envStr = getenv("EZ_APPKEY")){
            this->appKey = string(envStr);
        }
        if(envStr = getenv("EZ_APPSECRET")){
            this->appSecret = string(envStr);
        }
        if(envStr = getenv("EZ_VIDEO_DIR")){
            this->videoDir = string(envStr);
        }
        if(envStr = getenv("EZ_AMQP_ADDR")){
            this->amqpConfig.amqpAddr = string(envStr);
        }
        if(envStr = getenv("EZ_AMQP_EXCH")){
            this->amqpConfig.exchangeName = string(envStr);
        }
        if(envStr = getenv("EZ_AMQP_QUEUE")){
            this->amqpConfig.queName = string(envStr);
        }
        if(envStr = getenv("EZ_AMQP_ROUTE")){
            this->amqpConfig.routeKey = string(envStr);
        } 
    }
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