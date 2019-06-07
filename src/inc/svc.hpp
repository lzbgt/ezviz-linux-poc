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
#include <filesystem>
#include <sstream>
#include <cpp_redis/cpp_redis>
#include "amqp/handler.hpp"
#include "json.hpp"
#include "common.hpp"
// TODO: #include glog

using json = nlohmann::json;
namespace fs = std::filesystem;
using namespace std;

#define OPENADDR "https://open.ys7.com"
#define ENV_VIDEO_DIR "YS_VIDEO_DIR"
#define DEFAULT_VIDEO_DIR "videos"
// TODO: glog
#define log_if_return(...)
#define log_if_exit(...)

using namespace std;


typedef struct DEVICE_INFO_EX {
    ST_ES_DEVICE_INFO base;
    string uuid;
}DEVICE_INFO_EX;

class EZVizVideoService {
private:
    const int PRIORITY_PLAYBACK = 1;
    const int PRIORITY_RTPLAY = 10;
    const int NUM_THREAD_CLOUD = 4;

    unsigned long long runCount = 0;
    condition_variable cvDetach, cvReady;
    mutex mutDetach, mutReady;

    safe_vector<EZJobDetail> jobs;
    safe_vector<DEVICE_INFO_EX> jobsRTPlay;
    json statRTPlay;

    atomic<int> numRTPlayRunning = 0;


    uv_loop_t* uvLoop = NULL;
    EZAMQPHandler *ezAMQPHandler = NULL;
    EnvConfig envConfig = {};
    string ezvizToken = "at.bg2xm8xf03z5ygp01y84xxmv36z54txj-4n5jmc9bua-0iw2lll-qavzt882f";
    AMQP::Address *amqpAddr =NULL;
    AMQP::TcpConnection *amqpConn = NULL;
    AMQP::TcpChannel *chanPlayback = NULL, *chanRTPlay =NULL, *chanRTStop = NULL, *chanRTStop_ =NULL;
    cpp_redis::client redisClient;

    string ReqEZVizToken(string appKey, string appSecret)
    {
        return "at.5f1j87n71t54g5xg0wqjsw3r0ecke16v-60s30e4ide-17ydmfr-dbymgvf2z";
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
            // mqArgs["x-max-priority"] = PRIORITY_PLAYBACK;
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
            channel->declareExchange(this->envConfig.amqpConfig.rtplayExchangeName, AMQP::direct).onError([](const char*msg) {
                cerr << "error declare rtplay exchange: " << msg << endl;
            });
            AMQP::Table mqArgs;
            // mqArgs["x-max-priority"] = PRIORITY_RTPLAY;
            // mqArgs["x-expires"] = 10 * 1000; // 10s
            channel->declareQueue(this->envConfig.amqpConfig.rtplayQueName, 0, mqArgs).onError([](const char*msg) {
                cerr << "error declare rtplay queue: " << msg << endl;
            });
            channel->bindQueue(this->envConfig.amqpConfig.rtplayExchangeName,
            this->envConfig.amqpConfig.rtplayQueName, this->envConfig.amqpConfig.rtplayRouteKey).onError([](const char* msg) {
                cerr << "error bind rtplay queue: " << msg <<  endl;
            });

            // create _rtstop channle
            this->chanRTStop_ = new AMQP::TcpChannel(this->amqpConn);
            channel = this->chanRTStop_;
            // declare rtplay queue
            channel->declareExchange(this->envConfig.amqpConfig.rtstopExchangeName_, AMQP::direct).onError([](const char*msg) {
                cerr << "error declare rtstop_ exchange: " << msg << endl;
            });
            AMQP::Table mqArgs2;
            // mqArgs["x-max-priority"] = PRIORITY_RTPLAY;
            // mqArgs["x-expires"] = 10 * 1000; // 10s
            channel->declareQueue(this->envConfig.amqpConfig.rtstopQueName_, 0, mqArgs2).onError([](const char*msg) {
                cerr << "error declare rtplay queue: " << msg << endl;
            });
            channel->bindQueue(this->envConfig.amqpConfig.rtstopExchangeName_,
            this->envConfig.amqpConfig.rtstopQueName_, this->envConfig.amqpConfig.rtstopRouteKey_).onError([](const char* msg) {
                cerr << "error bind rtstop_ queue: " << msg <<  endl;
            });

            // create rtstop channle
            this->chanRTStop = new AMQP::TcpChannel(this->amqpConn);
            channel = this->chanRTStop;
            channel->declareExchange(this->envConfig.amqpConfig.rtstopExchangeName, AMQP::topic).onError([](const char*msg) {
                cerr << "error declare rtstop exchange: " << msg << endl;
            });
            // declare playback queue
            channel->declareQueue(this->envConfig.amqpConfig.rtstopQueName, 0).onError([](const char*msg) {
                cerr << "error declare rtstop queue: " << msg << endl;
            });
            channel->bindQueue(this->envConfig.amqpConfig.rtstopExchangeName,
                               this->envConfig.amqpConfig.rtstopQueName, this->envConfig.amqpConfig.rtstopRouteKey);
        }

        return ret;
    }

    int InitRedis() {
        this->redisClient.connect(this->envConfig.redisAddr, this->envConfig.redisPort, [](const std::string& host, std::size_t port, cpp_redis::client::connect_state status) {
            if (status == cpp_redis::client::connect_state::dropped) {
                std::cout << "client disconnected from " << host << ":" << port << std::endl;
            }
        }, 1000*2, -1, 1000*3);

                // set alive key
        auto set_ = this->redisClient.set(this->envConfig.amqpConfig.rtstopRouteKey, "1");
        auto exp_ = this->redisClient.pexpire(this->envConfig.amqpConfig.rtstopRouteKey, 1000*7); // 7s
        this->redisClient.sync_commit();
        cout << "alive key set on redis in 7s: " << this->envConfig.amqpConfig.rtstopRouteKey << endl;

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
        for(int i = 0; i < num; i++) {
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
        static int cnt = 0;

        auto ezvizDataCb = [](HANDLE pHandle, unsigned int dataType, unsigned char *buf, unsigned int buflen, void *pUser) ->int {
            EZCallBackUserData *cbd = (EZCallBackUserData *)pUser;
            while(cnt %1000 == 0) {
                cnt++;
                cout << "=====> data h: " << pHandle << " datatype: " << dataType << " pd: " << pUser << endl;
            }
            
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
            threads[i] = thread([this,i, ezvizMsgCb, ezvizDataCb]() {
                // loop infinitely for rtplay job
                EZCallBackUserData cbd;
                cbd.stat = 1;
                ES_STREAM_CALLBACK scb = {ezvizMsgCb, ezvizDataCb, (void *)&cbd};
                while(true) {
                    if(this->jobsRTPlay.size() > 0) {
                        // double check
                        auto dev = this->jobsRTPlay.pop_back();
                        if(dev.uuid == "") {
                            continue;
                        }
                        int ret = 0;
                        char tmStr[15] = {0};
                        string devSn= dev.base.szDevSerial;
                        string filename = this->envConfig.videoDir + "/";
                        time_t currTime = time(NULL);
                        tm *now = localtime(&currTime);
                        strftime(tmStr, sizeof(tmStr), "%Y%m%d%H%M%S", now);
                        filename += string(dev.base.szDevSerial) + "_" + tmStr + ".mpg";
                        ofstream *fout = new ofstream();
                        fout->open(filename, ios_base::binary | ios_base::trunc);
                        cout << "filename: " << filename << endl;
                        cbd.fout = fout;

                        cout << "params: " << this->ezvizToken << ", dev:" << dev.base.szDevSerial << ", " << dev.base.szSafeKey
                        << ", " << dev.base.iDevChannelNo << endl;

                        HANDLE handle = NULL;
                        ret = ESOpenSDK_StartRealPlay(this->ezvizToken.c_str(), dev.base, scb, handle);
                        if(ret != 0) {
                            cbd.fout->close();
                            delete cbd.fout;
                            this->statRTPlay.erase(devSn);
                            //TODO:
                            string key = this->RedisMakeRTPlayKey(devSn, dev.uuid);
                            RedisGet(key);
                            RedisDelete(key);
                            this->numRTPlayRunning--;
                            continue;
                        }
                        // wait for stop cmd or timeout
                        while(true) {
                            // stop
                            if(cbd.stat == 0 || this->statRTPlay[devSn].get<EZCMD>() == EZCMD::RTSTOP){
                                ESOpenSDK_StopPlayBack(handle);
                                cbd.fout->flush();
                                cbd.fout->close();

                                delete cbd.fout;
                                this->statRTPlay.erase(devSn);
                                //TODO:
                                string key = this->RedisMakeRTPlayKey(devSn, dev.uuid);
                                RedisGet(key);
                                RedisDelete(key);
                                this->numRTPlayRunning--;
                                break;
                            }
                            // check expiration

                            this_thread::sleep_for(3s);
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
    void SendAMQPMsg(AMQP::TcpChannel *chan, string &exchange, string &routekey, const char *msg) {
        chan->startTransaction().onError([](const char* msg){
            cout << "startTransaction MQ message error: " << msg << endl;
        });
        chan->publish(exchange, routekey, msg);
        chan->commitTransaction().onSuccess([]{
            cout << "commit MQ message success" << endl;
        }).onError([](const char* msg){
            cout <<"commit  MQ message error: " << msg <<endl;
        });
    }

    string RedisGet(string key) {
        // // moc
        // string sn =  key.substr(0,9);
        // cout << "sn: " << sn << endl;
        // if(this->statRTPlay.contains(sn)) {
        //     return this->envConfig.amqpConfig.rtstopRouteKey;
        // }
        auto get = this->redisClient.get(key);
        this->redisClient.sync_commit();
        string value = "";
        auto r = get.get();
        if(r.is_string()) {
            value = r.as_string();
        }
        cout << "redis get : " << key << "->" << value <<endl;
        return value;
    }

    string RedisPut(string key, string value) {
        auto get_ = this->redisClient.get(key);
        auto set_ = this->redisClient.set(key, value);
        auto exp_ = this->redisClient.pexpire(key, 1000 * 60 * 30); // 30 minutes
        this->redisClient.sync_commit();
        string old="";
        auto r = get_.get();
        if(r.is_string()) {
            old = r.as_string();
        }
        
        cout << "redis set : " << key  << " changed[" << old <<"->" << value<< "]" << endl;
        return old;
    }

    void RedisDelete(string key) {
        // TODO: implement soft deletion?
        cout << "redis del: " << key << endl;
        auto del_ = this->redisClient.del(vector<string>({{key}}));
        this->redisClient.sync_commit();
        del_.get();
    }

    int RedisExpireMs(string key, int ms) {
        auto pexprire = this->redisClient.pexpire(key, ms);
        this->redisClient.sync_commit();
        return 0;
    }


    string RedisMakeRTPlayKey(string devSn, string uuid) {
        return "mqrk:"+devSn+":"+uuid;
    }

    /**
     *
     * type: 1 - token, 2 - amqp; 4 - ezviz;
     *
     */
    void _init(int type_)
    {
        this->envConfig.toString();
        if(type_ & 1) {
            this->ezvizToken = ReqEZVizToken(envConfig.appKey, envConfig.appSecret);
            if (!fs::exists(this->envConfig.videoDir))
            {
                if (!fs::create_directory(this->envConfig.videoDir))
                {
                    cout << "can't create directory: " << this->envConfig.videoDir << endl;
                    exit(1);
                }
                fs::permissions(this->envConfig.videoDir, fs::perms::all);
            }
        }

        this->InitRedis();

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

    EZCMD VerifyAMQPMsg(ST_ES_DEVICE_INFO &dev, json &devJson) {
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
            devCode = devJson["devCode"].get<string>();
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
            cout << "uuid: " << uuid << endl;
            if(uuid.empty()) {
                ezCmd = EZCMD::NONE;
            }
        }else{
            ezCmd = EZCMD::NONE;
        }
        return ezCmd;
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

            // TOOD:
            // build ezviz download task
            // send to ezviz downloader

            delete msg;
        };


        // external api;
        auto OnRTPlayMessage = [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
            size_t len = message.bodySize();
            char *msg = new char[len+1];
            msg[len] = 0;
            memcpy(msg, message.body(), len);
            // acknowledge the message
            cout << "[====== OnRTPlayMessage: " << msg << endl;

            // parse
            ST_ES_DEVICE_INFO dev = {};

            string s = string(msg);
            json devJson = json::parse(s);

            EZCMD ezCmd = VerifyAMQPMsg(dev, devJson);
            string devSn, devCode, uuid;
            int chanId = 1;

            devSn = devJson["devSn"];
            devCode = devJson["devCode"];
            uuid = devJson["uuid"];
            chanId = devJson["chanId"].get<int>();

            if(EZCMD::RTPLAY != ezCmd){
                cerr << "\tinvalid messge " << endl;
            }else{
                cout << "check if this dev is in recording..." << endl;
                // query redis 
                string routekey = RedisGet(RedisMakeRTPlayKey(devSn, uuid));
                // check redis for existing job
                if(routekey.empty()) {
                    // new capture
                    cout << "\tno existing recording. create new on this instance" << endl;
                    this->jobsRTPlay.push_back(DEVICE_INFO_EX{dev, uuid});
                    string res = RedisPut(this->RedisMakeRTPlayKey(devSn, uuid),  this->envConfig.amqpConfig.rtstopRouteKey);
                    this->statRTPlay[devSn] = EZCMD::RTPLAY;
                    this->numRTPlayRunning++;
                    // message flow control
                    if(this->numRTPlayRunning >= this->envConfig.numConcurrentDevs){
                        //TODO: stop consume
                    }
                }else{
                    // existed
                    if(routekey == this->envConfig.amqpConfig.rtstopRouteKey) {
                        cout << "\talready recording on this instance, ingnore the message: " << routekey << endl;
                    }else{
                        // check alive
                        cout << "\talready recording on another instance: " << routekey << endl;
                        if(this->RedisGet(routekey) == "") {
                            cout << "\t\tbut it was a dead job before, create new on this instance" << endl;
                            this->jobsRTPlay.push_back(DEVICE_INFO_EX{dev, uuid});
                            string res = RedisPut(this->RedisMakeRTPlayKey(devSn, uuid),  this->envConfig.amqpConfig.rtstopRouteKey);
                            this->statRTPlay[devSn] = EZCMD::RTPLAY;
                            this->numRTPlayRunning++;
                            // message flow control
                            if(this->numRTPlayRunning >= this->envConfig.numConcurrentDevs){
                                //TODO: stop consume
                            }
                        }else{
                            cout << "\t\t and it's still running. ignore this message" << endl;
                        }
                    }
                }
            }

            // default ACK
            this->chanRTPlay->ack(deliveryTag);
            cout << "]====== End OnRTPlayMessage\n\n";
        };

        // external api
        auto OnRTStopMessage_ = [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
            size_t len = message.bodySize();
            char *msg = new char[len+1];
            msg[len] = 0;
            memcpy(msg, message.body(), len);
            string s = string(msg);
            json devJson = json::parse(s);
            cout << "[======OnRTStopMessage_: " << msg << endl;
            ST_ES_DEVICE_INFO dev = {};
            EZCMD ezCmd = VerifyAMQPMsg(dev,devJson);
            string devSn, devCode, uuid;
            int chanId = 1;

            devSn = devJson["devSn"];
            devCode = devJson["devCode"];
            uuid = devJson["uuid"];
            chanId = devJson["chanId"].get<int>();

            if(EZCMD::RTSTOP != ezCmd) {
                cerr << "\tinvalid messge " << endl;
            }else{
                cout << "check if this dev is in recording..." << endl;
                // query redis 
                string routekey = RedisGet(RedisMakeRTPlayKey(devSn, uuid));
                // check redis for existing job
                if(routekey.empty()) {
                    // no instance
                    cout << "\tno existing recording. ignore this message" << endl;
                }else{
                    // existed on this instance
                    if(routekey == this->envConfig.amqpConfig.rtstopRouteKey) {
                        cout << "\trecording on this instance, try to stop" << routekey << endl;
                        if(this->statRTPlay.contains(devSn)) {
                            this->statRTPlay[devSn] = EZCMD::RTSTOP;
                        }else{
                            cout << "\t\tbut can't find any running job on this instance, ignored" << endl;
                        }
                    }else{
                        cout << "\trerouting rtstop message to the recording instance: " << routekey << endl;
                        SendAMQPMsg(this->chanRTStop, this->envConfig.amqpConfig.rtstopExchangeName, routekey, msg);     
                    }
                }
            }

            // default ACK
            this->chanRTStop_->ack(deliveryTag);
            cout << "]======OnRTStopMessage_ " << endl;
        };

        // internal api
        auto OnRTStopMessage = [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
            size_t len = message.bodySize();
            char *msg = new char[len+1];
            msg[len] = 0;
            memcpy(msg, message.body(), len);
            string s = string(msg);
            json devJson = json::parse(s);
            cout << "[======OnRTStopMessage: " << msg << endl;
            ST_ES_DEVICE_INFO dev = {};
            EZCMD ezCmd = VerifyAMQPMsg(dev,devJson);
            string devSn, devCode, uuid;
            int chanId = 1;

            devSn = devJson["devSn"];
            devCode = devJson["devCode"];
            uuid = devJson["uuid"];
            chanId = devJson["chanId"].get<int>();

            if(ezCmd != EZCMD::RTSTOP) {
                cout << "error msg to process: " << msg << "\n\texpected a rtstop msg\n";
            }else{
                if(this->statRTPlay.contains(devSn)) {
                    this->statRTPlay[devSn] = EZCMD::RTSTOP;
                }else{
                    cout << "error rtstop, no running recording on this instance." << endl;
                }
            }

            // acknowledge the message
            cout << "]======OnRTStopMessage: " << endl;
            
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

            // stop_ queue (external)
            this->chanRTStop_->consume(this->envConfig.amqpConfig.rtstopQueName_)
            .onReceived(OnRTStopMessage_)
            .onSuccess(OnChanOperationStart)  //  channel operation start event, eg. starting heartbeat
            .onError(OnChanOperationFailed);   // channel operation failed event. eg. failed heartbeating?

            // stop queue (internal)
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

        // thread redis alive
        thread alive = thread([this](){
            while(true){
                this_thread::sleep_for(5s);
                this->RedisExpireMs(this->envConfig.amqpConfig.rtstopRouteKey, 1000*7);
            }
        });
        alive.detach();

        // thread worker
        thread worker = thread([this](){
            int concurrent = 4;
            thread *threads = new thread[concurrent];
            // if(this->envConfig.mode == EZMODE::PLAYBACK) {
            //     BootStrapDownloader(threads, concurrent);
            // }
            
            if(this->envConfig.mode == EZMODE::RTPLAY) {
                BootStrapRTPlay(threads, concurrent);
            }
        });
        if(worker.joinable()) {
            worker.detach();
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