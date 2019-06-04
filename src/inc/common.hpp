#ifndef __MY_COMMON_H__
#define __MY_COMMON_H__

#include <string>
#include <mutex>
#include <fstream>
using namespace std;

typedef struct EZAMPQConfig {
    string amqpAddr;
    string exchangeName;
    string queName;
    string routeKey;
} EZAMPQConfig;

typedef struct EnvConfig {
    string mode; /* EZ_MODE: playback, rtplay */
    string appKey; /* EZ_APPKEY:  */
    string appSecret; /* EZ_APPSECRET */
    string videoDir; /* EZ_VIDEO_DIR */
    EZAMPQConfig amqpConfig; /* EZ_AMQP_ADDR, EZ_AMQP_EXCH, EZ_AMQP_QUEUE, EZ_AMQP_ROUTE */
    int ezvizNumTcpThreadsMax; /* EZ_NUM_TCPTHREADS */
    int ezvizNumSslThreadsMax; /* EZ_NUM_SSLTHREADS */
    static void GetEnvConfig(EnvConfig& _this){
        char *envStr;
        if(envStr = getenv("EZ_MODE")){
            _this.mode = string(envStr);
        }
        if(envStr = getenv("EZ_APPKEY")){
            _this.appKey = string(envStr);
        }
        if(envStr = getenv("EZ_APPSECRET")){
            _this.appSecret = string(envStr);
        }
        if(envStr = getenv("EZ_VIDEO_DIR")){
            _this.videoDir = string(envStr);
        }
        if(envStr = getenv("EZ_AMQP_ADDR")){
            _this.amqpConfig.amqpAddr = string(envStr);
        }
        if(envStr = getenv("EZ_AMQP_EXCH")){
            _this.amqpConfig.exchangeName = string(envStr);
        }
        if(envStr = getenv("EZ_AMQP_QUEUE")){
            _this.amqpConfig.queName = string(envStr);
        }
        if(envStr = getenv("EZ_AMQP_ROUTE")){
            _this.amqpConfig.routeKey = string(envStr);
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