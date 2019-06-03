/*=============================================================================
#  Author:           blu (bruce.lu)
#  Email:            lzbgt@126.com 
#  FileName:         main.cpp
#  Description:      /
#  Version:          0.0.1
#  History:         
=============================================================================*/
#ifndef __MY_EZVIZ_H__
#define __MY_EZVIZ_H__

#include "amqp/handler.hpp"
#include <ESOpenStream.h>
#include "common.hpp"
// TODO: #include glog

#define OPENADDR "https://open.ys7.com"
#define ENV_VIDEO_DIR "YS_VIDEO_DIR"
#define DEFAULT_VIDEO_DIR "videos"
// TODO: glog
#define log_if_return(...)
#define log_if_exit(...)

using namespace std;

class EZVizVideoService {
    private:
    const int PRIORITY_PLAYBACK = 1;
    const int PRIORITY_RTRECORD = 10;

    AMQPHandler rabbitHandler;
    EnvConfig envConfig;
    string ezvizToken;

    private:
    string ReqEZVizToken(string appKey, string appSecret){
        return "";
    }

    int InitEZViz(){
        int ret = 0;
        ret = ESOpenSDK_Init(envConfig.ezvizNumTcpThreadsMax, envConfig.ezvizNumSslThreadsMax);
        log_if_exit(0!=ret, "failed to init ezviz sdk", true);
        ESOpenSDK_InitWithAppKey(envConfig.appKey.c_str(), OPENADDR);

        return ret;
    }

    int InitAMQP() {
        int ret = 0;
        // // address of the server
        AMQP::Address address(this->envConfig.amqpAddr);

        // create a AMQP connection object
        AMQP::TcpConnection connection(&rabbitHandler, address);

        // and create a channel
        AMQP::TcpChannel channel(&connection);

        // declare playback queue
        channel.declareExchange(this->envConfig.amqpPlayBack.exchangeName);
        AMQP::Table mqArgs;
        mqArgs["x-max-priority"] = PRIORITY_PLAYBACK;
        channel.declareQueue(this->envConfig.amqpPlayBack.queName, AMQP::durable + AMQP::autodelete, mqArgs);
        channel.bindQueue(this->envConfig.amqpPlayBack.exchangeName, 
            this->envConfig.amqpPlayBack.queName, this->envConfig.amqpPlayBack.routeKey);

        // declare rtrecord queue
        channel.declareExchange(this->envConfig.amqpRTRecord.exchangeName);
        mqArgs["x-max-priority"] = PRIORITY_RTRECORD;
        channel.declareQueue(this->envConfig.amqpRTRecord.queName, AMQP::durable + AMQP::autodelete, mqArgs);
        channel.bindQueue(this->envConfig.amqpRTRecord.exchangeName, 
            this->envConfig.amqpRTRecord.queName, this->envConfig.amqpRTRecord.routeKey);

        return ret;
    }

    int EZVizMsgCb(HANDLE pHandle, int code, int eventType, void *pUser)
    {
        cout << "=====> msg h: " << pHandle << " code: " << code << " evt: " << eventType << " pd: " << pUser << endl;
        EZCallBackUserData *cbd = (EZCallBackUserData *)pUser;
        
        if (code == ES_STREAM_CLIENT_RET_OVER || eventType != ES_STREAM_EVENT::ES_NET_EVENT_CONNECTED)
        {
            if (cbd != NULL)
            {
                cbd->stat = 0;
            }
        }

        return 0;
    }

    int EZVizDataCb(HANDLE pHandle, unsigned int dataType, unsigned char *buf, unsigned int buflen, void *pUser)
    {
        EZCallBackUserData *cbd = (EZCallBackUserData *)pUser;
        if (ES_STREAM_TYPE::ES_STREAM_DATA == dataType)
        {
            // force sequential writing when multi-threading in EZVizSDK (normal case)
            cbd->m.lock();
            cbd->fout->write(reinterpret_cast<const char *>(buf), buflen);
            cbd->m.unlock();
        }
        else if (ES_STREAM_TYPE::ES_STREAM_END == dataType)
        {
            if (cbd != NULL)
            {
                cbd->stat = 0;
            }
        }

        return 0;
    }
    
    public:
    // ctor
    EZVizVideoService(EnvConfig envConfig) {
        // get env:
        //      threads config
        //      dir config
        //      rabbitmq config
        //      ezviz config
        // init ezviz sdk
        // init rabbitmq conn
        // request token
        this->envConfig = envConfig;
        this->ezvizToken = ReqEZVizToken(envConfig.appKey, envConfig.appSecret);
        this->InitEZViz();
        this->InitAMQP();
    }

    // entry
    void Run() {

    }

    // dtor
    ~EZVizVideoService(){

    }
};

#endif