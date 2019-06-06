/*=============================================================================
#  Author:           blu (bruce.lu)
#  Email:            lzbgt@icloud.com
#  FileName:         main.cpp
#  Description:      /
#  Version:          0.0.1
#  History:
=============================================================================*/
#ifndef __MY_EZVIZ_SVC_H__
#define __MY_EZVIZ_SVC_H__

#include <chrono>
#include <ESOpenStream.h>
#include <atomic>
#include "amqp/handler.hpp"
#include "json.hpp"
#include "common.hpp"
// TODO: #include glog

using json = nlohmann::json;

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
    const int NUM_THREAD_CLOUD = 4;

    unsigned long long runCount = 0;
    condition_variable cvDetach, cvReady;
    mutex mutDetach, mutReady;

    safe_vector<EZJobDetail> jobs;
    safe_vector<ST_ES_DEVICE_INFO> jobsRTPlay;
    json statRTPlay;

    atomic<int> numRTPlayRunning = 0;


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
        cout << "mode: " << this->envConfig.mode << endl;
        if(this->uvLoop != NULL) {
            cerr << "reconnect ..." << endl;
            delete this->uvLoop;
        }
        this->uvLoop = new uv_loop_t;
        uv_loop_init(this->uvLoop);
        this->ezAMQPHandler = new EZAMQPHandler(&(this->cvReady), &(this->cvDetach), this->uvLoop);
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
                cerr << "error :" << msg << endl;
            });
            AMQP::Table mqArgs;
            mqArgs["x-max-priority"] = PRIORITY_PLAYBACK;
            channel->declareQueue(this->envConfig.amqpConfig.playbackQueName, AMQP::durable, mqArgs).onError([](const char *msg) {
                cerr << "error: " << msg << endl;
            });
            channel->bindQueue(this->envConfig.amqpConfig.playbackExchangeName,
            this->envConfig.amqpConfig.playbackQueName, this->envConfig.amqpConfig.playbackRouteKey).onError([](const char * msg) {
                cerr << "error: " << msg << endl;
            });
        }

        // rtplay
        if(this->envConfig.mode & EZMODE::RTPLAY) {
            // create rtplay channle
            this->chanRTPlay = new AMQP::TcpChannel(this->amqpConn);
            channel = this->chanRTPlay;
            // declare rtplay queue
            // channel->declareExchange(this->envConfig.amqpConfig.rtplayExchangeName, AMQP::direct, AMQP::durable + AMQP::autodelete).onError([](const char*msg) {
            //     cerr << "error declare rtplay exchange: " << msg << endl;
            // });
            AMQP::Table mqArgs;
            mqArgs["x-max-priority"] = PRIORITY_RTPLAY;
            mqArgs["x-expires"] = 10 * 1000; // 10s
            channel->declareQueue(this->envConfig.amqpConfig.rtplayQueName, AMQP::autodelete & (~AMQP::durable), mqArgs).onError([](const char*msg) {
                cerr << "error declare rtplay exchange: " << msg << endl;
            });
            channel->bindQueue(this->envConfig.amqpConfig.rtplayExchangeName,
            this->envConfig.amqpConfig.rtplayQueName, this->envConfig.amqpConfig.rtplayRouteKey).onError([](const char* msg) {
                cerr << "error decalre rtplay queue: " << msg <<  endl;
            });

            // create rtstop channle
            this->chanRTStop = new AMQP::TcpChannel(this->amqpConn);
            channel = this->chanRTStop;
            // channel->declareExchange(this->envConfig.amqpConfig.rtstopExchangeName, AMQP::topic, AMQP::durable + AMQP::autodelete).onError([](const char*msg) {
            //     cerr << "error declare rtstop exchange: " << msg << endl;
            // });
            // declare playback queue
            channel->declareQueue(this->envConfig.amqpConfig.rtstopQueName, AMQP::autodelete & (~AMQP::durable)).onError([](const char*msg) {
                cerr << "error declare rtstop queue: " << msg << endl;
            });
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

    void DownloadOneFile(ST_ES_DEVICE_INFO &dev, ST_ES_RECORD_INFO &di, string appKey, string token)
    {
        ES_RECORD_INFO *rip = &di;
        int ret = 0;
        tm tm1 = {}, tm2 = {};
        char tmStr[15] = {};
        strptime(rip->szStartTime, "%Y-%m-%d %H:%M:%S", &tm1);
        strptime(rip->szStopTime, "%Y-%m-%d %H:%M:%S", &tm2);
        time_t t1 = mktime(&tm1), t2 = mktime(&tm2);
        int secs = difftime(t2, t1);
        cout << "secs: " << secs << endl;
        strftime(tmStr, sizeof(tmStr), "%Y%m%d%H%M%S", &tm1);
        string filename = tmStr;
        filename = this->envConfig.videoDir + "/" + filename;
        filename += string("_") + string(dev.szDevSerial) + "_" + to_string(secs) + ".mpg";
        cout << "filename: " << filename << endl;
        ofstream *fout = new ofstream();
        fout->open(filename, ios_base::binary | ios_base::trunc);
        cout << "file opened: " << filename << endl;
        EZCallBackUserData cbd;
        cbd.fout = fout;
        cbd.stat = 1;
        ES_STREAM_CALLBACK scb = {NULL, NULL, (void *)&cbd};
        HANDLE handle = NULL;
        ret = ESOpenSDK_StartPlayBack(token.c_str(), dev, *rip, scb, handle);
        if (0 != ret) {
            delete cbd.fout;
            return;
        }

        ESOpenSDK_StopPlayBack(handle);
        cbd.fout->flush();
        cbd.fout->close();
        delete cbd.fout;
    }

    void BootStrapDownloader(thread *threads, int num)
    {
        // create download threads, and loop over tasks for ever
        for(int i = 0; i < 4; i++) {
            threads[i] = thread([this] {
                while (this->jobs.size() > 0)
                {
                    // download one record
                    EZJobDetail reci = this->jobs.pop_back();
                    if (reci.devsn[0] != 0) {
                        int ret = 0;
                        char tmStr[15] = {0};
                        thread th[5];
                        // TODO: concurrently download cloud file
                        for(int i = 0; i < 4; i++) {

                        }
                        // SD card file
                        auto sdt = thread([&reci]() {
                            for(int j = 0; j < reci.fileNumSD; j++) {
                                //
                            }
                        });
                    }
                }// while
            });
        }
    }

    // rtplay
    void BootStrapRTPlay(thread *threads, int num)
    {
        auto ezvizMsgCb = [](HANDLE pHandle, int code, int eventType, void *pUser) ->int{
            cout << "=====> msg h: " << pHandle << " code: " << code << " evt: " << eventType << " pd: " << pUser << endl;
            EZCallBackUserData *cbd = (EZCallBackUserData *)pUser;

            if (code == ES_STREAM_CLIENT_RET_OVER || eventType != ES_STREAM_EVENT::ES_NET_EVENT_CONNECTED)
            {
                if (cbd != NULL) {
                    cbd->stat = 0;
                }
            }

            return 0;
        };

        auto ezvizDataCb = [](HANDLE pHandle, unsigned int dataType, unsigned char *buf, unsigned int buflen, void *pUser) ->int {
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
                if (cbd != NULL) {
                    cbd->stat = 0;
                }
            }

            return 0;
        };

        //
        for(int i = 0; i < num; i++) {
            threads[i] = thread([this, ezvizMsgCb, ezvizDataCb]() {
                // loop infinitely for rtplay job
                while(true) {
                    if(this->jobsRTPlay.size() > 0) {
                        auto dev = this->jobsRTPlay.pop_back();
                        int ret = 0;
                        char tmStr[15] = {0};
                        string filename = this->envConfig.videoDir + "/";
                        time_t currTime = time(NULL);
                        tm *now = localtime(&currTime);
                        strftime(tmStr, sizeof(tmStr), "%Y%m%d%H%M%S", now);
                        filename += string(dev.szDevSerial) + "_" + tmStr + ".mpg";
                        ofstream *fout = new ofstream();
                        fout->open(filename, ios_base::binary | ios_base::trunc);
                        cout << "filename: " << filename << endl;
                        EZCallBackUserData cbd;
                        ES_STREAM_CALLBACK scb = {ezvizMsgCb, ezvizDataCb, (void *)&cbd};
                        HANDLE handle = NULL;
                        cbd.fout = NULL;
                        cbd.stat = 1;
                        ret = ESOpenSDK_StartRealPlay(this->ezvizToken.c_str(), dev, scb, handle);
                        // wait for stop cmd or timeout
                        while(true) {

                        }
                    }
                    else {
                        //snap for new job
                        this_thread::sleep_for(100ms);
                    }
                }
            });
        }
    }
    //
    void SendAMQPMsg(AMQP::TcpChannel *ch, string &exchange, string &routekey, const char *msg) {
        AMQP::Channel * chan = ch;
        chan->startTransaction().onError([](const char* msg){
            cout << "startTransaction error: " << msg << endl;
        });
        chan->publish(exchange, routekey, msg);
        chan->commitTransaction().onSuccess([]{
            cout << "commit success" << endl;
        }).onError([](const char* msg){
            cout <<"commit error: " << msg <<endl;
        });
    }

    string RedisGet(string key) {
        return "";
    }

    string RedisPut(string key, string value) {
        return "";
    }

    /**
     *
     * type: 1 - token, 2 - amqp; 4 - ezviz;
     *
     */
    void _init(int type_)
    {
        if(type_ & 1) {
            this->ezvizToken = ReqEZVizToken(envConfig.appKey, envConfig.appSecret);
        }

        if(type_ & 2) {
            this->InitAMQP();
        }

        if(type_ & 4) {
            this->InitEZViz();
        }
    }

    void _free()
    {
        // TODO: release all resources
        uv_stop(this->uvLoop);
        uv_loop_close(this->uvLoop);
        // TODO: ?
        // ESOpenSDK_Fini();
        if(chanPlayback != NULL)
            delete chanPlayback;
        if(chanRTPlay != NULL)
            delete chanRTPlay;
        if(chanRTStop != NULL)
            delete chanRTStop;
        if(chanRTStop != NULL)
            delete chanRTStop;
        if(amqpConn != NULL)
            delete amqpConn;
        if(amqpAddr != NULL) {
            delete amqpAddr;
        }
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
        _init(7);
    }
    // dtor
    ~EZVizVideoService()
    {
        _free();
    }

    // entry
    void Run()
    {
        // reconnect on network issue
        cout <<"runCount: " << this->runCount << endl;
        if(runCount > 0) {
            this->_init(7);
            runCount++;
            if(runCount == 0) {
                runCount = 1;
            }
        }

        thread *t = new thread([this] {
            uv_run(this->uvLoop, UV_RUN_DEFAULT);
        });

        // detach thread
        if(t->joinable()) {
            t->detach();
        }

        auto OnChanOperationStart = [](const string &consumertag) {
            // cout << "consume operation started: " << consumertag << std::endl;
        };

        // callback function that is called when the consume operation failed
        auto OnChanOperationFailed = [](string message) {
            cout << "consume operation failed: " << message << std::endl;
        };

        // callback operation when a message was received
        auto OnPlaybackMessage = [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
            size_t len = message.bodySize();
            char *msg = new char[len+1];
            msg[len] = 0;
            memcpy(msg, message.body(), len);
            // acknowledge the message
            cout << "OnPlaybckMessage: " << msg << endl;
            this->chanPlayback->ack(deliveryTag);

            // build ezviz download task
            // send to ezviz downloader

            delete msg;
        };


        // callback operation when a message was received
        auto OnRTPlayMessage = [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
            size_t len = message.bodySize();
            char *msg = new char[len+1];
            msg[len] = 0;
            memcpy(msg, message.body(), len);
            // acknowledge the message
            cout << "OnRTPlayMessage: " << msg << endl;

            // parse
            ST_ES_DEVICE_INFO dev = {};
            bool valid = true;
            json devJson = json::parse(msg);

            EZCMD ezCmd = EZCMD::NONE;
            string cmd, devSn, devCode, uuid;
            int chanId = 1;
            if(devJson.contains("cmd")) {
                cmd = devJson["cmd"].get<string>();
                if(cmd == "rtplay" ) {
                    ezCmd = EZCMD::RTPLAY;
                }else if(cmd == "rtstop") {
                    ezCmd = EZCMD::RTSTOP;
                }
            }

            if(devJson.contains("devSn")) {
                devSn = devJson["devSn"].get<string>();
                strncpy(dev.szDevSerial, devSn.c_str(), sizeof(dev.szDevSerial));
            }else{
                ezCmd = EZCMD::NONE;
            }

            if(devJson.contains("devCode")) {
                devCode = devJson["devcode"].get<string>();
                strncpy(dev.szSafeKey, devCode.c_str(), sizeof(dev.szSafeKey));
            }else{
                ezCmd= EZCMD::NONE;
            }

            if(devJson.contains("chanId")) {
                chanId = devJson["chanId"].get<int>();
                dev.iDevChannelNo = chanId;
            }else{
                ezCmd = EZCMD::NONE;
            }

            if(devJson.contains("uuid")) {
                uuid = devJson["uuid"].get<string>();
                if(uuid.empty()) {
                    ezCmd = EZCMD::NONE;
                }
            }else{
                ezCmd = EZCMD::NONE;
            }

            if(EZCMD::NONE == ezCmd) {
                cerr << "invalid messge: " << msg << endl;
            }else{
                // query redis 
                string routekey = RedisGet(devSn + "." + uuid + ".routekey");
                if(routekey.empty()) {
                    // new capture
                    if(ezCmd == EZCMD::RTSTOP) {
                        // wrong cmd
                        cerr << "no running instance to stop: " << msg << endl;
                    }else{
                        this->jobsRTPlay.push_back(dev);
                        string res = RedisPut(devSn + "." + uuid + ".routekey",  this->envConfig.amqpConfig.rtstopRouteKey);
                        this->statRTPlay[devSn] = EZCMD::RTPLAY;
                        this->numRTPlayRunning++;
                    }
                }else{
                    // has capturing instance
                    if(routekey == this->envConfig.amqpConfig.rtstopRouteKey) { // on this instance
                        if(ezCmd == EZCMD::RTPLAY){
                            // requeue to other instances
                            cerr << "already capturing on this instance, requeue to other instance: " << msg << endl;
                            this->chanRTPlay->reject(deliveryTag);
                            return;
                        }else{
                            // RTSTOP
                            this->statRTPlay[devSn] = EZCMD::RTSTOP;
                        }
                    }else{
                        // on other instance
                        // reroute to its instance
                        SendAMQPMsg(this->chanRTStop,this->envConfig.amqpConfig.rtstopExchangeName,routekey, msg);
                    }
                }
            }

            // default ACK
            this->chanRTPlay->ack(deliveryTag);
        };

        // callback operation when a message was received
        auto OnRTStopMessage = [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
            cout << "rtstop message received: " << (char*)(message.body()) << endl;
            // acknowledge the message
            this->chanRTStop->ack(deliveryTag);
        };

        // check run mode
        if(this->envConfig.mode == EZMODE::PLAYBACK) {
            // start consuming from the queue, and install the callbacks
            this->chanPlayback->consume(this->envConfig.amqpConfig.playbackQueName)
            .onReceived(OnPlaybackMessage)
            .onSuccess(OnChanOperationStart)  //  channel operation start event, eg. starting heartbeat
            .onError(OnChanOperationFailed);   // channel operation failed event. eg. failed heartbeating?
        }
        else if(this->envConfig.mode == EZMODE::RTPLAY) {
            // play queue
            this->chanRTPlay->consume(this->envConfig.amqpConfig.rtplayQueName)
            .onReceived(OnRTPlayMessage)
            .onSuccess(OnChanOperationStart)  //  channel operation start event, eg. starting heartbeat
            .onError(OnChanOperationFailed);   // channel operation failed event. eg. failed heartbeating?

            // stop queue
            this->chanRTStop->consume(this->envConfig.amqpConfig.rtstopQueName)
            .onReceived(OnRTStopMessage)
            .onSuccess(OnChanOperationStart)  //  channel operation start event, eg. starting heartbeat
            .onError(OnChanOperationFailed);   // channel operation failed event. eg. failed heartbeating?
        }
        else {
            //
            cerr << "invalid run mode, exiting ..." << endl;
            exit(1);
        }

        // check for channel ready
        std::unique_lock<std::mutex> lk_ready(mutReady);
        auto stat = this->cvReady.wait_for(lk_ready, 7s);
        if(cv_status::timeout == stat) {
            cerr << "channel not usable, resetting..." << endl;
            _free();
            this->_init(7);
            return;
        }

        int concurrent = 4;
        thread *threads = new thread[concurrent];
        if(this->envConfig.mode == EZMODE::PLAYBACK) {
            BootStrapDownloader(threads, concurrent);
        }
        else if(this->envConfig.mode == EZMODE::RTPLAY) {
            BootStrapRTPlay(threads, concurrent);
        }

        // check network status and do heartbeating
        while(true) {
            unique_lock<std::mutex> lk_detach(mutDetach);
            stat = this->cvDetach.wait_for(lk_detach, 7s);
            if(cv_status::no_timeout == stat) {
                cerr << "network issue, resetting..." << endl;
                _free();
                break;
            }
            // cout << "heartbeating ..." << endl;
            this->amqpConn->heartbeat();
        }
    }
};

#endif