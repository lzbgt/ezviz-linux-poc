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
#include <chrono>
#include <functional>
#include "amqp/handler.hpp"
#include "json.hpp"
#include "common.hpp"
// TODO: #include glog

using json = nlohmann::json;
namespace fs = std::filesystem;
using namespace std;
using namespace std::chrono;


#define OPENADDR "https://open.ys7.com"
#define ENV_VIDEO_DIR "YS_VIDEO_DIR"
#define DEFAULT_VIDEO_DIR "videos"
// TODO: glog
#define log_if_return(...)
#define log_if_exit(...)

using namespace std;
using namespace std::placeholders;


typedef struct DEVICE_INFO_EX {
    ST_ES_DEVICE_INFO base;
    string uuid;
    uint64_t deliveryTag; // ack when downloading completed
    EZCMD cmd;
    int duration;
} DEVICE_INFO_EX;

class EZVizVideoService {
private:
    const int PRIORITY_PLAYBACK = 1;
    const int PRIORITY_RTPLAY = 10;
    const int NUM_THREAD_CLOUD = 4;
    const string REDIS_KEY_CTN_JOBS = "rtplay_ctn_jobs";

    unsigned long long runCount = 0;
    condition_variable cvDetach, cvReady;
    mutex mutDetach, mutReady;

    safe_vector<EZJobDetail> jobs;
    safe_vector<DEVICE_INFO_EX> jobsRTPlay;
    json statRTPlay;
    mutex mutStatRTPlay;

    atomic<int> numRTPlayRunning = 0;
    bool bMessageDone = true;


    uv_loop_t* uvLoop = NULL;
    EZAMQPHandler *ezAMQPHandler = NULL;
    EnvConfig envConfig = {};
    string ezvizToken = "at.bg2xm8xf03z5ygp01y84xxmv36z54txj-4n5jmc9bua-0iw2lll-qavzt882f";
    AMQP::Address *amqpAddr =NULL;
    AMQP::TcpConnection *amqpConn = NULL;
    AMQP::TcpChannel *chanPlayback = NULL, *chanRTPlay =NULL, *chanRTStop = NULL, *chanRTStop_ =NULL;
    cpp_redis::client redisClient;

    string ReqEZVizToken()
    {
        while(true){
            string body = "appKey="+ this->envConfig.appKey + "&appSecret=" + this->envConfig.appSecret;
            spdlog::info("getting ezviz token ...");
            string token;
            string res = myutils::HTTPPostRequest(string(OPENADDR) + "/api/lapp/token/get", body, map<string,string> {});
            spdlog::info("response: {}", res);
            try {
                json jres = json::parse(res);
                if(jres.count("data") == 0 ||jres["data"].size() == 0 || jres["data"].count("accessToken") == 0) {
                    spdlog::error("failed to request yscloud token, retry ..."); 
                    this_thread::sleep_for(chrono::seconds(3));
                    continue;
                }
                token = jres["data"]["accessToken"];
            }
            catch(exception e) {
                spdlog::error("req token exception: {}", e.what());
                exit(1);
            }
            spdlog::info("token: {}", token);
            if(token.empty()) {
                spdlog::info("failed to get token, exiting ...");
                exit(1);
            }
            this->ezvizToken = token;
            return token;
        }
        
        //return "at.5f1j87n71t54g5xg0wqjsw3r0ecke16v-60s30e4ide-17ydmfr-dbymgvf2z";
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
        spdlog::info("mode: {}",this->envConfig.mode);
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
            spdlog::info("setting up playback channel");
            this->chanPlayback = new AMQP::TcpChannel(this->amqpConn);
            channel = this->chanPlayback;
            // declare playback queue
            channel->declareExchange(this->envConfig.amqpConfig.playbackExchangeName, AMQP::direct).onError([](const char* msg) {
                cerr << "error :" << msg << endl;
            });
            AMQP::Table mqArgs;
            // mqArgs["x-max-priority"] = PRIORITY_PLAYBACK;
            channel->declareQueue(this->envConfig.amqpConfig.playbackQueName, AMQP::durable, mqArgs).onError([](const char *msg) {
                cerr << "on declaureQueue error: " << msg << endl;
            });
            channel->bindQueue(this->envConfig.amqpConfig.playbackExchangeName,
            this->envConfig.amqpConfig.playbackQueName, this->envConfig.amqpConfig.playbackRouteKey).onError([](const char * msg) {
                cerr << "on buildQueue error: " << msg << endl;
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
            // set max prefetch to EZ_BATCH_SIZE
            channel->setQos(this->envConfig.numConcurrentDevs, false);

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
            channel->declareQueue(this->envConfig.amqpConfig.rtstopQueName, AMQP::autodelete).onError([](const char*msg) {
                cerr << "error declare rtstop queue: " << msg << endl;
            });
            channel->bindQueue(this->envConfig.amqpConfig.rtstopExchangeName,
                               this->envConfig.amqpConfig.rtstopQueName, this->envConfig.amqpConfig.rtstopRouteKey);
        }

        return ret;
    }

    int InitRedis()
    {
        this->redisClient.connect(this->envConfig.redisAddr, this->envConfig.redisPort, [](const std::string& host, std::size_t port, cpp_redis::client::connect_state status) {
            if (status == cpp_redis::client::connect_state::dropped) {
                spdlog::error("client disconnected from {}:{}", host, port);
            }
        }, 1000*2, -1, 1000*3);

        // set alive key
        auto set_ = this->redisClient.set(this->envConfig.amqpConfig.rtstopRouteKey, "1");
        auto exp_ = this->redisClient.pexpire(this->envConfig.amqpConfig.rtstopRouteKey, 1000*7); // 7s
        this->redisClient.sync_commit();
        spdlog::info("alive key set on redis in 7s: {}", this->envConfig.amqpConfig.rtstopRouteKey);

        return 0;
    }

    // rtplay
    void BootStrapRTPlay(thread *threads, int num)
    {
        auto ezvizMsgCb = [](HANDLE pHandle, int code, int eventType, void *pUser) ->int{
            EZCallBackUserData *cbd = (EZCallBackUserData *)pUser;

            if (code == ES_STREAM_CLIENT_RET_OVER || eventType != ES_STREAM_EVENT::ES_NET_EVENT_CONNECTED)
            {
                if (cbd != NULL) {
                    cbd->stat = 0;
                }
                spdlog::warn("msg h: {} code: {}, evt: {}, pd: {}", pHandle, code, eventType, pUser);
            }
            return 0;
        };
        static int cnt = 0;

        auto ezvizDataCb = [](HANDLE pHandle, unsigned int dataType, unsigned char *buf, unsigned int buflen, void *pUser) ->int {
            EZCallBackUserData *cbd = (EZCallBackUserData *)pUser;
            while(cnt %1000 == 0)
            {
                cnt++;
                spdlog::info("msg h: {} datatype: {}, len: {}, pd: {}", pHandle, dataType, buflen, pUser);
            }

            if (cbd!= NULL && (ES_STREAM_TYPE::ES_STREAM_DATA == dataType))
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
                spdlog::warn("stream end from yscloud");
            }

            cbd->bytesWritten += buflen;
            return 0;
        };

        //
        for(int i = 0; i < num; i++) {
            threads[i] = thread([this,i, ezvizMsgCb, ezvizDataCb]() {
                // loop infinitely for rtplay job
                EZCallBackUserData cbd;
                cbd.stat = 1;
                cbd.bytesWritten = 0;
                cbd.numRetried = 0;
                ES_STREAM_CALLBACK scb = {ezvizMsgCb, ezvizDataCb, (void *)&cbd};
                int64_t nSnapCnt =0;
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
                        filename += string(tmStr) + "_" + string(dev.base.szDevSerial) + ".mp4";
                        ofstream *fout = new ofstream();
                        fout->open(filename, ios_base::binary | ios_base::trunc);
                        cbd.fout = fout;
                        EZCMD ezCmd = dev.cmd;

                        spdlog::info("record params filename: {}, token: {}, sn: {}, code: {}, chanId: {}", filename, this->ezvizToken, dev.base.szDevSerial, dev.base.szSafeKey, dev.base.iDevChannelNo);

                        HANDLE handle = NULL;
                        ret = ESOpenSDK_StartRealPlay(this->ezvizToken.c_str(), dev.base, scb, handle);
                        if(ret != 0) {
                            spdlog::error("{} ESOpenSDK_StartRealPlay ret: \n\n{}\n\n", devSn, ret);
                            cbd.fout->close();
                            delete cbd.fout;
                            {
                                auto lg = lock_guard(this->mutStatRTPlay);
                                this->statRTPlay[devSn]= (int)EZCMD::NONE;
                            }
                            
                            string key = this->RedisMakeRTPlayKey(devSn, dev.uuid);
                            RedisDelete(key);
                            this->numRTPlayRunning--;
                            if(ezCmd == EZCMD::RTPLAY_CTN){
                                if(ret == 302003 || ret == 320007 || ret == 510121 || ret == 525404 || ret == 550012) {
                                    this->chanRTPlay->reject(dev.deliveryTag, AMQP::requeue);
                                    spdlog::warn("device {} is not online, keep retry. open retcode: {}", devSn, ret);
                                }else{
                                    this->chanRTPlay->ack(dev.deliveryTag);
                                    spdlog::error("ignore device: {}, open retcode: {}", devSn, ret);
                                }
                                
                            }else{
                                this->chanRTPlay->ack(dev.deliveryTag);
                                spdlog::error("ignore device: {}. open retcode: {}", devSn, ret);
                            }
                            
                            continue;
                        }
                        // wait for stop cmd or timeout
                        auto chro_start = high_resolution_clock::now();
                        unsigned long long sizeDownloaded = cbd.bytesWritten;
                        bool bNoData = false;
                        int64_t nWaitCnt = 0;
                        while(true) {
                            // check to stop
                            // timeout of job
                            nWaitCnt++;
                            string routekey = this->RedisGet(this->RedisMakeRTPlayKey(devSn, dev.uuid));

                            if(bNoData || cbd.stat == 0 || this->statRTPlay[devSn].get<int>() == EZCMD::RTSTOP||routekey.empty()) {
                                // stop and upload
                                ESOpenSDK_StopPlayBack(handle);
                                cbd.fout->flush();
                                cbd.fout->close();

                                delete cbd.fout;
                                // upload
                                if(this->envConfig.uploadProgPath.empty() || this->envConfig.apiSrvAddr.empty()) {
                                    spdlog::warn("no external upload program configured for uploading");
                                }
                                else {
                                    if( cbd.bytesWritten == 0) {
                                        spdlog::error("{} video file empty ignored. please check network connections. \twill not try to automatically connect to this camera when using continous recording\n\n", devSn);
                                        system((string("rm -f ") + filename).c_str());
                                    }else{
                                        string program = string("nohup ") + this->envConfig.uploadProgPath + string(" -s ") + this->envConfig.apiSrvAddr +  string(" -i ") + filename + string(" &");
                                        spdlog::info("upload video full command line: {}", program);
                                        system(program.c_str());
                                    }
                                }
                                
                                // check if need continue to record
                                if(this->statRTPlay[devSn].get<int>() != EZCMD::RTSTOP && ezCmd == EZCMD::RTPLAY_CTN && cbd.bytesWritten != 0) {
                                    this->chanRTPlay->reject(dev.deliveryTag, AMQP::requeue);
                                }else if(ezCmd == EZCMD::RTPLAY_CTN && cbd.bytesWritten == 0){
                                    ESOpenSDK_Fini();
                                    _init(4);
                                }else{
                                    this->chanRTPlay->ack(dev.deliveryTag);
                                }

                                this->statRTPlay[devSn] = (int)EZCMD::NONE;
                                string key = this->RedisMakeRTPlayKey(devSn, dev.uuid);
                                this->RedisDelete(key);
                                this->RedisSRem(this->REDIS_KEY_CTN_JOBS, key);
                                this->numRTPlayRunning--;
                                
                                if(this->numRTPlayRunning < this->envConfig.numConcurrentDevs) {
                                    // TODO: resume is not supported in the lib
                                    // play queue
                                }
                                break;  
                            }else{
                                // check expiration
                                // TODO: wait on signal
                                if(nWaitCnt % 10 == 0) {
                                    spdlog::info("watting for job {} to compete: {}", devSn, nWaitCnt);
                                }
                                
                                this_thread::sleep_for(7s); // it has job, so can sleep for a long time.
                                if(cbd.bytesWritten == sizeDownloaded) {
                                    bNoData = true;
                                    spdlog::error("{} no data in 7s. please check server/camera network connections! will stop and retry", devSn);
                                    continue;
                                }
                                auto duora = duration_cast<seconds>(high_resolution_clock::now() - chro_start);
                                if(duora.count() >= 60) {
                                    spdlog::info("{} speed EST: {} KB/s", filename, (cbd.bytesWritten - sizeDownloaded) / (duora.count() * 1024.0 + 1));
                                    // reset size
                                    sizeDownloaded = cbd.bytesWritten;
                                    // reset time start
                                    chro_start = high_resolution_clock::now();
                                }
                            }
                        }
                    }
                    else {
                        //snap for new job
                        nSnapCnt++;
                        this_thread::sleep_for(500ms);
                        if(nSnapCnt % 1000 == 0) {
                            spdlog::info("snap for new job");
                        }
                        
                    }
                }
            });
            threads[i].detach();
        }
    }
    //
    void SendAMQPMsg(AMQP::TcpChannel *chan, string &exchange, string &routekey, const char *msg)
    {
        chan->startTransaction().onError([](const char* msg) {
            spdlog::error("startTransaction MQ message error: {}",msg);
        });
        chan->publish(exchange, routekey, msg);
        chan->commitTransaction().onSuccess([] {
            spdlog::debug("commit MQ message success");
        }).onError([](const char* msg) {
            spdlog::error("commit  MQ message error: {}",msg);
        });
    }

    string RedisGet(string key, bool bPrint = false)
    {
        auto get = this->redisClient.get(key);
        this->redisClient.sync_commit();
        string value = "";
        auto r = get.get();
        if(r.is_string()) {
            value = r.as_string();
        }
        if(bPrint) {
            spdlog::info("redis get: {} -> {}", key, value);
        }

        return value;
    }

    string RedisPut(string key, string value, int expms = 1000 * 60 * 30)
    {
        auto get_ = this->redisClient.get(key);
        auto set_ = this->redisClient.set(key, value);
        auto exp_ = this->redisClient.pexpire(key, expms); // 30 minutes
        this->redisClient.sync_commit();
        string old="";
        auto r = get_.get();
        if(r.is_string()) {
            old = r.as_string();
        }

        spdlog::info("redis set: {} -> {}. old v: {} ", key , value, old);
        return old;
    }

    int RedisSAdd(string key, string val){
        vector<string> vals = {val};
        auto f_ = redisClient.sadd(key, vals);
        redisClient.sync_commit();
        auto r_ = f_.get();
        if(r_.is_integer()) {
            return r_.as_integer();
        }else{
            return 0;
        }
    }

    vector<string> RedisSMembers(string key) {
        vector<string> ret;
        try{
            auto get_ = redisClient.smembers(key);
            redisClient.sync_commit();
            auto r = get_.get();
            if(r.is_array()) {
                auto arr = r.as_array();
                for(auto &i:arr) {
                    if(i.is_string()) {
                        ret.push_back(i.as_string());
                    }                
                }
            }
        }catch(exception &e) {
            spdlog::error("excpetion smembers: {}", e.what());
        }
        
        return ret;
    }

    int RedisSRem(string key, string val) {
        vector<string> vals = {val};
        redisClient.srem(key, vals);
        redisClient.sync_commit();

        return 0;
    }

    // all recording jobs that long running
    vector<string> GetCtnJobs() {
        vector<string> ret;
        auto r = RedisSMembers(REDIS_KEY_CTN_JOBS);
        for(auto &i:r) {
           auto s = RedisGet(i);
           if(s.empty()) {
               ret.push_back(i);
           }
        }

        return ret;
    }

    void RedisDelete(string key)
    {
        // TODO: implement soft deletion?
        auto del_ = this->redisClient.del(vector<string>({{key}}));
        this->redisClient.sync_commit();
        del_.get();
    }

    int RedisExpireMs(string key, int ms)
    {
        auto pexprire = this->redisClient.pexpire(key, ms);
        this->redisClient.sync_commit();
        return 0;
    }

    string RedisMakeRTPlayKey(string devSn, string uuid)
    {
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
            this->ReqEZVizToken();
            if (!fs::exists(this->envConfig.videoDir)) {
                if (!fs::create_directory(this->envConfig.videoDir)) {
                    spdlog::error("can't create directory: {}", this->envConfig.videoDir);
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

    EZCMD VerifyAMQPMsg(ST_ES_DEVICE_INFO &dev, json &devJson)
    {
        EZCMD ezCmd = EZCMD::NONE;
        string cmd, devSn, devCode, uuid;
        int chanId = 1;

        try {
            if(devJson.contains("cmd")) {
                cmd = devJson["cmd"].get<string>();

                if(cmd == "rtplay" ) {
                    ezCmd = EZCMD::RTPLAY;
                }
                else if(cmd == "rtstop") {
                    ezCmd = EZCMD::RTSTOP;
                }else if(cmd == "rtplay_continue") {
                    ezCmd = EZCMD::RTPLAY_CTN;
                }
            }

            if(devJson.contains("devSn")) {
                devSn = devJson["devSn"].get<string>();
                strncpy(dev.szDevSerial, devSn.c_str(), sizeof(dev.szDevSerial));
            }
            else {
                ezCmd = EZCMD::NONE;
            }

            if(devJson.contains("devCode")) {
                devCode = devJson["devCode"].get<string>();
                strncpy(dev.szSafeKey, devCode.c_str(), sizeof(dev.szSafeKey));
            }
            else {
                ezCmd= EZCMD::NONE;
            }

            if(devJson.contains("chanId")) {
                if(devJson.at("chanId").is_number_integer()) {
                    chanId = devJson["chanId"].get<int>();
                }
            }

            dev.iDevChannelNo = chanId;
            
            if(devJson.contains("uuid")) {
                uuid = devJson["uuid"].get<string>();
                if(uuid.empty()) {
                    ezCmd = EZCMD::NONE;
                }
            }
        }
        catch(exception e) {
            spdlog::error("exception in veryfiy msg {}: {}", devJson.dump(), e.what());
        }

        return ezCmd;
    }

    // external api; auto OnRTMessage = [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
    void Method_OnRTMessage(const AMQP::Message &message, uint64_t deliveryTag, bool redelivered)
    {
        size_t len = message.bodySize();
        char *msg = new char[len+1];
        msg[len] = 0;
        memcpy(msg, message.body(), len);
        // acknowledge the message
        spdlog::info("[====== OnRTMessage: {}", msg);

        // parse
        ST_ES_DEVICE_INFO dev = {};

        string s = string(msg);
        json devJson;
        int duration = 10*60;
        string devSn, devCode, uuid;
        int chanId = 1;
        EZCMD ezCmd;

        try {
            devJson = json::parse(s);
            devSn = devJson["devSn"];
            devCode = devJson["devCode"];
            uuid = devJson["uuid"];
            if(devJson.count("chanId") != 0) {
                if(devJson.at("chanId").is_number_integer()) {
                    chanId = devJson["chanId"].get<int>();
                }
            }

            if(devJson.count("duration") != 0) {
                if(devJson.at("duration").is_number_integer()) {
                    duration = devJson["duration"].get<int>();
                }else{
                    spdlog::warn("duratoin is not a number. use default");
                }
            }else{
                spdlog::warn("no duration configured. use default");
            }
 
            ezCmd = VerifyAMQPMsg(dev, devJson);

            if(EZCMD::RTPLAY == ezCmd ||EZCMD::RTPLAY_CTN == ezCmd) {
                spdlog::info("check if this dev is in recording...");
                // query redis
                string routekey = RedisGet(RedisMakeRTPlayKey(devSn, uuid));
                // check redis for existing job
                if(routekey.empty()) {
                    // new capture
                    // message flow control
                    spdlog::info("running jobs on this instance: {}, allowed max: {}", this->numRTPlayRunning ,this->envConfig.numConcurrentDevs);
                    if(this->numRTPlayRunning >= this->envConfig.numConcurrentDevs) {
                        //TODO: stop consume, pause is not IMPLEMENTED in the library, use cancel instead
                        // this->chanRTPlay->pause();
                        spdlog::warn("\tflow control, reject & cancel consumming");
                        this->chanRTPlay->reject(deliveryTag, AMQP::requeue);
                        // this->chanRTPlay->cancel(this->envConfig.amqpConfig.rtstopRouteKey);
                        return;
                    }

                    spdlog::info("\tno existing recording. create new on this instance");

                    
                    this->jobsRTPlay.push_back(DEVICE_INFO_EX{dev, uuid, deliveryTag, ezCmd, duration});
                    string res = RedisPut(this->RedisMakeRTPlayKey(devSn, uuid),  this->envConfig.amqpConfig.rtstopRouteKey, duration*1000);
                    {
                        auto lg = lock_guard(this->mutStatRTPlay);
                        this->statRTPlay[devSn] = (int)EZCMD::RTPLAY;
                    }
                    
                    if(ezCmd == EZCMD::RTPLAY_CTN) {
                        RedisSAdd(this->REDIS_KEY_CTN_JOBS, this->RedisMakeRTPlayKey(devSn, uuid));
                    }
                    this->numRTPlayRunning++;
                    // no ack
                    return;
                }
                else {
                    // existed
                    if(routekey == this->envConfig.amqpConfig.rtstopRouteKey) {
                        spdlog::warn("\talready recording on this instance {}, ingnore the message: {}", routekey, msg);
                    }
                    else {
                        // check alive
                        spdlog::info("\talready recording on another instance: {}. msg: {}", routekey, msg);
                        if(this->RedisGet(routekey) == "") {
                            spdlog::info("\t\tbut it was a dead job before, try createing new on this instance");
                            spdlog::info("running jobs on this instance: {}, allowed max: {}", this->numRTPlayRunning , this->envConfig.numConcurrentDevs);
                            // message flow control
                            if(this->numRTPlayRunning >= this->envConfig.numConcurrentDevs) {
                                //TODO: stop consume, pause is not IMPLEMENTED in the library, use reject instead
                                // this->chanRTPlay->pause();
                                spdlog::info("\tflow control, reject & cancel consumming");
                                // this->chanRTPlay->reject(deliveryTag, AMQP::requeue);
                                // this->chanRTPlay->cancel(this->envConfig.amqpConfig.rtstopRouteKey);
                                if(EZCMD::RTPLAY_CTN == ezCmd) {
                                     this->chanRTPlay->reject(deliveryTag, AMQP::requeue);
                                }else{
                                    this->chanRTPlay->ack(deliveryTag);
                                }
                                
                                return;
                            }
                            this->jobsRTPlay.push_back(DEVICE_INFO_EX{dev, uuid, deliveryTag, ezCmd});
                            string res = RedisPut(this->RedisMakeRTPlayKey(devSn, uuid),  this->envConfig.amqpConfig.rtstopRouteKey, duration*1000);
                            {
                                auto lg = lock_guard(this->mutStatRTPlay);
                                this->statRTPlay[devSn] = (int)EZCMD::RTPLAY;
                            }
                            
                            this->numRTPlayRunning++;
                            // no ack
                            return;
                        }
                        else {
                            spdlog::info("\t\t and it's still running, ask to stop, and requeuethis message");
                            // TODO:...
                            // avoiding continue-recording-messge loss when having signle instance crashed
                            json msg;
                            msg["cmd"] = "rtstop";
                            msg["chanId"] = 1;
                            msg["devSn"] = devSn;
                            msg["devCode"] = devCode;
                            msg["uuid"] = uuid;
                            msg["quality"] = 0;

                            RedisExpireMs(this->RedisMakeRTPlayKey(devSn, uuid), 0);
                            // SendAMQPMsg(this->chanRTStop, this->envConfig.amqpConfig.rtstopExchangeName, routekey, msg.dump().c_str());
                            
                            this->chanRTPlay->reject(deliveryTag, AMQP::requeue);
                            return;
                        }
                    }
                }
            }else if(EZCMD::RTSTOP == ezCmd){
                spdlog::info("rtstop check if this dev is in recording...");
                // query redis
                string routekey = RedisGet(RedisMakeRTPlayKey(devSn, uuid));
                // check redis for existing job
                if(routekey.empty()) {
                    // no instance
                    spdlog::info("Method_OnRTMessage no existing recording. ignore this message");
                }
                else {
                    // existed on this instance
                    if(routekey == this->envConfig.amqpConfig.rtstopRouteKey) {
                        spdlog::info("\trecording on this instance {}, try to stop ", routekey);
                        {
                            auto lg = lock_guard(this->mutStatRTPlay);
                            if(this->statRTPlay.contains(devSn) && this->statRTPlay[devSn].get<int>() != EZCMD::NONE) {
                                this->statRTPlay[devSn] = (int)EZCMD::RTSTOP;
                            }
                            else {
                                spdlog::info("\t\tbut can't find any running job on this instance, ignored");
                            }  
                        }
                        
                    }
                    else {
                        if(this->RedisGet(routekey).empty()) {
                            spdlog::info("\tthe recording instance {} is dead, drop the message and delete the job", routekey);
                            RedisDelete(RedisMakeRTPlayKey(devSn, uuid));
                        }
                        else {
                            spdlog::info("\trerouting rtstop message to the recording instance {}", routekey);
                            SendAMQPMsg(this->chanRTStop, this->envConfig.amqpConfig.rtstopExchangeName, routekey, msg);
                        }
                    }
                }
            }else{
                spdlog::error("OnRTMessage invalid messge");
            }

            this->chanRTStop_->ack(deliveryTag);
            return;

        }
        catch(exception e) {
            // default ACK
            this->chanRTPlay->ack(deliveryTag);
            spdlog::error("exception in parse message json, ignore message: {}\n]====== End OnRTMessage", e.what());
            return;
        }

        // no default ACK
        this->chanRTPlay->ack(deliveryTag);
        spdlog::info("]====== End OnRTMessage");
    }   


    // internal api; auto OnRTStopMessage = [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
    void Method_OnRTStopMessage(const AMQP::Message &message, uint64_t deliveryTag, bool redelivered)
    {
        size_t len = message.bodySize();
        char *msg = new char[len+1];
        msg[len] = 0;
        memcpy(msg, message.body(), len);
        string s = string(msg);
        spdlog::info("[======OnRTStopMessage: {}", msg);
        ST_ES_DEVICE_INFO dev = {};
        json devJson;
        string devSn, devCode, uuid;
        int chanId = 1;

        try {
            devJson = json::parse(s);
            devSn = devJson["devSn"];
            devCode = devJson["devCode"];
            uuid = devJson["uuid"];
            if(devJson.count("chanId") != 0) {
                if(devJson.at("chanId").is_number_integer()) {
                    chanId = devJson["chanId"].get<int>();
                }
            }
        }
        catch(exception e) {
            // default ACK
            spdlog::error("exception parsing message json: {}\n]====== End OnRTStopMessage", e.what());
            this->chanRTStop->ack(deliveryTag);
            return;
        }

        EZCMD ezCmd = VerifyAMQPMsg(dev,devJson);

        if(ezCmd != EZCMD::RTSTOP) {
            spdlog::error("Method_OnRTStopMessage invalid msg to process: {}" ,msg);
        }
        else {
            auto lg = lock_guard(this->mutStatRTPlay);
            if(this->statRTPlay.contains(devSn) && this->statRTPlay[devSn].get<int>() != EZCMD::NONE) {
                this->statRTPlay[devSn] = (int)EZCMD::RTSTOP;
            }
            else {
                spdlog::error("Method_OnRTStopMessage error rtstop, no running recording on this instance: {}", devSn);
            }
        }

        // acknowledge the message
        spdlog::info("]======OnRTStopMessage");

        this->chanRTStop->ack(deliveryTag);
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
        spdlog::info("network reset count: {}",this->runCount);
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
            spdlog::error("consume operation failed: {}" , message );
        };

        // callback operation when a message was received
        auto OnPlaybackMessage = [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
            size_t len = message.bodySize();
            char *msg = new char[len+1];
            msg[len] = 0;
            memcpy(msg, message.body(), len);
            // acknowledge the message
            spdlog::info("OnPlaybckMessage: {}", msg);
            this->chanPlayback->ack(deliveryTag);

            // TOOD:
            // build ezviz download task
            // send to ezviz downloader

            delete msg;
        };

        auto OnRTMessage = bind(&EZVizVideoService::Method_OnRTMessage, this, _1, _2, _3);
        auto OnRTStopMessage = bind(&EZVizVideoService::Method_OnRTStopMessage, this, _1, _2, _3);
        // check run mode
        if(this->envConfig.mode == EZMODE::PLAYBACK) {
            // // start consuming from the queue, and install the callbacks
            // this->chanPlayback->consume(this->envConfig.amqpConfig.playbackQueName)
            // .onReceived(OnPlaybackMessage)
            // .onSuccess(OnChanOperationStart)  //  channel operation start event, eg. starting heartbeat
            // .onError(OnChanOperationFailed);   // channel operation failed event. eg. failed heartbeating?
        }
        else if(this->envConfig.mode == EZMODE::RTPLAY) {
            // // play queue
            this->chanRTPlay->consume(this->envConfig.amqpConfig.rtplayQueName)
            .onReceived(OnRTMessage)
            .onSuccess(OnChanOperationStart)  //  channel operation start event, eg. starting heartbeat
            .onError(OnChanOperationFailed);   // channel operation failed event. eg. failed heartbeating?

            // stop_ queue (external)
            this->chanRTStop_->consume(this->envConfig.amqpConfig.rtstopQueName_)
            .onReceived(OnRTMessage)
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
        thread alive = thread([this]() {
            static long long cnt = 0;
            const long long interval = 4 * 60 * 60;
            while(true) {
                cnt++;
                if(cnt % interval == 0) {
                    //refresh token
                    this->ReqEZVizToken();
                }
                this_thread::sleep_for(4s);
                this->RedisExpireMs(this->envConfig.amqpConfig.rtstopRouteKey, 1000*7);
            }
        });
        alive.detach();

        // thread worker
        thread worker = thread([this]() {
            thread *threads = new thread[this->envConfig.numConcurrentDevs];

            if(this->envConfig.mode == EZMODE::RTPLAY) {
                BootStrapRTPlay(threads, this->envConfig.numConcurrentDevs);
            }
        });
        if(worker.joinable()) {
            worker.detach();
        }

        // check network status and do heartbeating
        int firstRun = 0;
        while(true) {
            firstRun++;    
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