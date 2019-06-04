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

#include <chrono>
#include <ESOpenStream.h>
#include "amqp/handler.hpp"
#include "json.hpp"
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
    const int PRIORITY_RTPLAY = 10;
    condition_variable cv_network;
    mutex cv_network_m;

    uv_loop_t* uvLoop = NULL;
    EZAMQPHandler *ezAMQPHandler = NULL;
    EnvConfig envConfig = {};
    string ezvizToken;
    AMQP::Address *amqpAddr =NULL;
    AMQP::TcpConnection *amqpConn = NULL;
    AMQP::TcpChannel *chanPlayback = NULL, *chanRTPlay =NULL, *chanRTStop = NULL, *_chanRTStop =NULL;

    string ReqEZVizToken(string appKey, string appSecret)
    {
        return "";
    }

    int InitEZViz()
    {
        int ret = 0;
        ret = ESOpenSDK_Init(envConfig.ezvizNumTcpThreadsMax, envConfig.ezvizNumSslThreadsMax);
        log_if_exit(0!=ret, "failed to init ezviz sdk", true);
        ESOpenSDK_InitWithAppKey(envConfig.appKey.c_str(), OPENADDR);

        return ret;
    }

    int InitAMQP()
    {
        int ret = 0;
        if(this->uvLoop != NULL) {
            cout << "retry ..." << endl;
            delete this->uvLoop;
        }
        this->uvLoop = new uv_loop_t;
        uv_loop_init(this->uvLoop);
        this->ezAMQPHandler = new EZAMQPHandler(&(this->cv_network), &(this->cv_network_m), this->uvLoop);
        cout << "mode: " << this->envConfig.mode << endl;
        // address of the server
        this->amqpAddr = new AMQP::Address(this->envConfig.amqpConfig.amqpAddr);
        // create a AMQP connection object
        this->amqpConn = new AMQP::TcpConnection(this->ezAMQPHandler, *(this->amqpAddr));

        AMQP::TcpChannel *channel = NULL;

        // playback
        if(this->envConfig.mode & EZMODE::PLAYBACK) {
            // and create a channel
            cout << "setting up playback channel" << endl;
            this->chanPlayback = new AMQP::TcpChannel(this->amqpConn);
            channel = this->chanPlayback;
            // declare playback queue
            channel->declareExchange(this->envConfig.amqpConfig.playbackExchangeName, AMQP::direct).onError([](const char* msg) {
                cout << "error :" << msg << endl;
            });
            AMQP::Table mqArgs;
            mqArgs["x-max-priority"] = PRIORITY_PLAYBACK;
            channel->declareQueue(this->envConfig.amqpConfig.playbackQueName, AMQP::durable, mqArgs).onError([](const char *msg) {
                cout << "error: " << msg << endl;
            });
            channel->bindQueue(this->envConfig.amqpConfig.playbackExchangeName,
            this->envConfig.amqpConfig.playbackQueName, this->envConfig.amqpConfig.playbackRouteKey).onError([](const char * msg) {
                cout << "error: " << msg << endl;
            });
        }

        // rtplay
        if(this->envConfig.mode & EZMODE::RTPLAY) {
            // create rtplay channle
            this->chanRTPlay = new AMQP::TcpChannel(this->amqpConn);
            channel = this->chanRTPlay;
            // declare playback queue
            channel->declareExchange(this->envConfig.amqpConfig.rtplayExchangeName);
            AMQP::Table mqArgs;
            mqArgs["x-max-priority"] = PRIORITY_RTPLAY;
            channel->declareQueue(this->envConfig.amqpConfig.rtplayQueName, AMQP::autodelete, mqArgs);
            channel->bindQueue(this->envConfig.amqpConfig.rtplayExchangeName,
                               this->envConfig.amqpConfig.rtplayQueName, this->envConfig.amqpConfig.rtplayRouteKey);

            // create rtstop channle
            this->chanRTStop = new AMQP::TcpChannel(this->amqpConn);
            channel = this->chanRTStop;
            // declare playback queue
            channel->declareExchange(this->envConfig.amqpConfig.rtstopExchangeName, AMQP::topic);
            channel->bindQueue(this->envConfig.amqpConfig.rtstopExchangeName,
                               this->envConfig.amqpConfig.rtstopQueName, this->envConfig.amqpConfig.rtstopRouteKey);
        }

        return ret;
    }

    int EZVizMsgCb(HANDLE pHandle, int code, int eventType, void *pUser)
    {
        cout << "=====> msg h: " << pHandle << " code: " << code << " evt: " << eventType << " pd: " << pUser << endl;
        EZCallBackUserData *cbd = (EZCallBackUserData *)pUser;

        if (code == ES_STREAM_CLIENT_RET_OVER || eventType != ES_STREAM_EVENT::ES_NET_EVENT_CONNECTED) {
            if (cbd != NULL) {
                cbd->stat = 0;
            }
        }

        return 0;
    }

    int EZVizDataCb(HANDLE pHandle, unsigned int dataType, unsigned char *buf, unsigned int buflen, void *pUser)
    {
        EZCallBackUserData *cbd = (EZCallBackUserData *)pUser;
        if (ES_STREAM_TYPE::ES_STREAM_DATA == dataType) {
            // force sequential writing when multi-threading in EZVizSDK (normal case)
            cbd->m.lock();
            cbd->fout->write(reinterpret_cast<const char *>(buf), buflen);
            cbd->m.unlock();
        }
        else if (ES_STREAM_TYPE::ES_STREAM_END == dataType) {
            if (cbd != NULL) {
                cbd->stat = 0;
            }
        }

        return 0;
    }

public:
    // ctor
    EZVizVideoService()
    {
        // get env:
        //      threads config
        //      dir config
        //      rabbitmq config
        //      ezviz config
        // init ezviz sdk
        // init rabbitmq conn
        // request token
        this->ezvizToken = ReqEZVizToken(envConfig.appKey, envConfig.appSecret);
        this->InitAMQP();
        this->InitEZViz();
    }
    // dtor
    ~EZVizVideoService()
    {
        //
    }

    // entry
    void Run()
    {
        cout <<"conn: " << this->amqpConn << endl;
        // AMQP::Channel * chan = this->chanPlayback;
        // chan->startTransaction().onError([](const char* msg){
        //     cout << "startTransaction error: " << msg << endl;
        // });
        // chan->publish(this->envConfig.amqpConfig.playbackExchangeName, this->envConfig.amqpConfig.playbackQueName, "hello world!");
        // chan->commitTransaction().onSuccess([]{
        //     cout << "commit success" << endl;
        // }).onError([](const char* msg){
        //     cout <<"commit error: " << msg <<endl;
        // });

        auto startCb = [](const std::string &consumertag) {

            std::cout << "consume operation started" << std::endl;
        };

        // callback function that is called when the consume operation failed
        auto errorCb = [](const char *message) {

            std::cout << "consume operation failed" << std::endl;
        };

        // callback operation when a message was received
        auto messageCb = [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {

            std::cout << "message received" << std::endl;

            // acknowledge the message
            this->chanPlayback->ack(deliveryTag);
        };
        thread *t = new thread([=, this] {
            uv_run(this->uvLoop, UV_RUN_DEFAULT);
        });
        while(true) {
            // start consuming from the queue, and install the callbacks
            this->chanPlayback->consume(this->envConfig.amqpConfig.playbackQueName)
            .onReceived(messageCb)
            .onSuccess(startCb)  // callback when consuming successfully
            .onError(errorCb);   // callback when consuming failed on message

            std::unique_lock<std::mutex> lk(cv_network_m);
            if(cv_status::timeout == this->cv_network.wait_for(lk, 7s)) {
                std::cerr << "timeout waiting for network issue, "  << "heartbeating ..." << endl;
                this->amqpConn->heartbeat();
            }
            else {
                std::cerr << "no timeout: network issue indeed occured, resetting..." << endl;
                this->InitAMQP();
                // delete t;
                t = new thread([=, this] {
                    uv_run(this->uvLoop, UV_RUN_DEFAULT);
                });
            }
        }
    }
};

#endif