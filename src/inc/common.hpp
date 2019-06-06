#ifndef __MY_COMMON_H__
#define __MY_COMMON_H__

#include <string>
#include <mutex>
#include <fstream>
#include <iostream>
#include "uuid.hpp"

using namespace std;

#define STR_PLAYBACK  "playback"
#define STR_RTPLAY  "rtplay"
#define STR_RTSTOP  "rtstop"


typedef struct EZAMPQConfig {
    string amqpAddr;
    string playbackExchangeName;
    string playbackQueName;
    string playbackRouteKey;
    //
    string rtplayExchangeName;
    string rtplayQueName;
    string rtplayRouteKey;
    //
    string rtstopExchangeName;
    string rtstopQueName;
    string rtstopRouteKey;
} EZAMPQConfig;

typedef enum EZMODE {
    NONE,
    PLAYBACK,
    RTPLAY,
    ALL
} EZMODE;

typedef struct EnvConfig {
    public:
    EZMODE mode; /* EZ_MODE: playback, rtplay */
    string appKey; /* EZ_APPKEY:  */
    string appSecret; /* EZ_APPSECRET */
    string videoDir; /* EZ_VIDEO_DIR */
    EZAMPQConfig amqpConfig; /* EZ_AMQP_ADDR, EZ_AMQP_EXCH, EZ_AMQP_QUEUE, EZ_AMQP_ROUTE */
    int numConcurrentDevs;
    int ezvizNumTcpThreadsMax; /* EZ_NUM_TCPTHREADS */
    int ezvizNumSslThreadsMax; /* EZ_NUM_SSLTHREADS */

    void _default_init(){
        this->mode = EZMODE::PLAYBACK;
        this->ezvizNumSslThreadsMax = 4;
        this->ezvizNumTcpThreadsMax = 4;
        this->videoDir = "videos";
        this->numConcurrentDevs = 4;
        //
        this->amqpConfig.amqpAddr = "amqp://guest:guest@172.20.10.6:5672/";
        this->amqpConfig.playbackExchangeName = "ezviz.exchange.default";
        this->amqpConfig.playbackQueName="ezviz.work.queue.playback";
        this->amqpConfig.playbackRouteKey = "playback";
        //
        this->amqpConfig.rtplayExchangeName = "ezviz.exchange.default";
        this->amqpConfig.rtplayQueName="ezviz.work.queue.rtplay";
        this->amqpConfig.rtplayRouteKey = "rtplay";
        //
        this->amqpConfig.rtstopExchangeName = "ezviz.exchange.realtime";
        this->amqpConfig.rtstopQueName="ezviz.work.queue.rtstop";
        this->amqpConfig.rtstopRouteKey = myutils::GenerateUUID('.');
        cout << this->amqpConfig.rtstopRouteKey << endl;
    }

    EnvConfig(){
        _default_init();
        char *envStr;

        if(envStr = getenv("EZ_MODE")){
            if(0 == memcmp(envStr, STR_RTPLAY, strlen(STR_RTPLAY))){
                this->mode = EZMODE::RTPLAY;
            }else if(0 == memcmp(envStr, STR_PLAYBACK, strlen(STR_PLAYBACK))){
                this->mode = EZMODE::PLAYBACK;
            }else {
                cout << "invalid mode: " << this->mode << endl;
                cout << "choices are: rtplay, playback" << endl;
                exit(1);
            }
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

        if(envStr = getenv("EZ_BATCH_SIZE")){
            this->numConcurrentDevs = stoi(string(envStr));
        }

        if(envStr = getenv("EZ_AMQP_ADDR")){
            this->amqpConfig.amqpAddr = string(envStr);
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


typedef ST_ES_RECORD_INFO * ST_ES_RECORD_INFO_PTR;

typedef struct EZJobDetail {
    string devsn;
    string devcode;
    ST_ES_RECORD_INFO_PTR *cloudVideos;
    ST_ES_RECORD_INFO_PTR *sdVideos;
    int fileNumCloud;
    int fileNumSD;
}EZJobDetail;

template <typename T>
class safe_vector
{
private:
    vector<T> vec = vector<T>{};
    static mutex _m;

public:
    void push_back(const T &v)
    {
        lock_guard<std::mutex> guard(_m);
        vec.push_back(v);
    }

    T pop_back()
    {
        lock_guard<std::mutex> guard(_m);
        T ret = T{};
        memset(&ret, 0, sizeof(T));
        if (vec.size() > 0)
        {
            ret = vec.back();
            vec.pop_back();
        }

        return ret;
    }

    size_t size()
    {
        return vec.size();
    }

    vector<T> &get()
    {
        return vec;
    }
};

template <typename T>
mutex safe_vector<T>::_m;

#endif