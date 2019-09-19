#ifndef __MY_COMMON_H__
#define __MY_COMMON_H__

#include <string>
#include <mutex>
#include <fstream>
#include <iostream>
#include "uuid.hpp"
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/Path.h>
#include <Poco/URI.h>
#include <Poco/Exception.h>
#include <sstream>

using namespace std;

#define STR_PLAYBACK  "playback"
#define STR_RTPLAY  "rtplay"
#define STR_RTSTOP  "rtstop"

#define MAX_PATH_NUM_CHARS 512


typedef struct EZAMPQConfig {
    string amqpAddr;
    string playbackExchangeName;
    string playbackQueName;
    string playbackRouteKey;
    // external interface
    string rtplayExchangeName;
    string rtplayQueName;
    string rtplayRouteKey;
    // external interface
    string rtstopExchangeName_;
    string rtstopQueName_;
    string rtstopRouteKey_;
    // internal only
    string rtstopExchangeName;
    string rtstopQueName;
    string rtstopRouteKey;
} EZAMPQConfig;

typedef enum EZMODE {
    NONE,
    PLAYBACK,
    RTPLAY,
    RTSTOP,
    RTPLAY_CTN,
    ALL
} EZMODE;

typedef EZMODE EZCMD;

typedef struct EnvConfig {
public:
    EZMODE mode; /* EZ_MODE: playback, rtplay */
    string appKey; /* EZ_APPKEY:  */
    string appSecret; /* EZ_APPSECRET */
    string videoDir; /* EZ_VIDEO_DIR */
    string apiSrvAddr;
    string uploadProgPath;
    EZAMPQConfig amqpConfig; /* EZ_AMQP_ADDR, EZ_AMQP_EXCH, EZ_AMQP_QUEUE, EZ_AMQP_ROUTE */
    string redisAddr;
    int redisPort;
    int numConcurrentDevs;
    int ezvizNumTcpThreadsMax; /* EZ_NUM_TCPTHREADS */
    int ezvizNumSslThreadsMax; /* EZ_NUM_SSLTHREADS */

    void _default_init()
    {
        this->mode = EZMODE::PLAYBACK;
        this->ezvizNumSslThreadsMax = 4;
        this->ezvizNumTcpThreadsMax = 4;
        this->videoDir = "videos";
        this->numConcurrentDevs = 4;
        //
        this->appKey = "a287e05ace374c3587e051db8cd4be82";
        this->appSecret = "f01b61048a1170c4d158da3752e4378d";
        //
        this->amqpConfig.amqpAddr = "amqp://guest:guest@127.0.0.1:5672/";
        this->amqpConfig.playbackExchangeName = "ezviz.exchange.playback";
        this->amqpConfig.playbackQueName="ezviz.work.queue.playback";
        this->amqpConfig.playbackRouteKey = "playback";
        //
        this->amqpConfig.rtplayExchangeName = "ezviz.exchange.rtplay";
        this->amqpConfig.rtplayQueName="ezviz.work.queue.rtplay";
        this->amqpConfig.rtplayRouteKey = "rtplay";
        //
        this->amqpConfig.rtstopExchangeName_ = "ezviz.exchange.rtplay";
        this->amqpConfig.rtstopQueName_="ezviz.work.queue.rtstop_";
        this->amqpConfig.rtstopRouteKey_ = "rtstop_";
        //
        this->amqpConfig.rtstopExchangeName = "ezviz.exchange.rtstop";
        this->amqpConfig.rtstopQueName="ezviz.work.queue.rtstop";
        this->amqpConfig.rtstopRouteKey = myutils::GenerateUUID('.');
        this->redisAddr = "127.0.0.1";
        this->redisPort = 6379;
    }

    EnvConfig()
    {
        _default_init();
        char *envStr;

        if(envStr = getenv("EZ_MODE")) {
            if(0 == memcmp(envStr, STR_RTPLAY, strlen(STR_RTPLAY))) {
                this->mode = EZMODE::RTPLAY;
            }
            else if(0 == memcmp(envStr, STR_PLAYBACK, strlen(STR_PLAYBACK))) {
                this->mode = EZMODE::PLAYBACK;
            }
            else {
                cout << "invalid mode: " << this->mode << endl;
                cout << "choices are: rtplay, playback" << endl;
                exit(1);
            }
        }

        if(envStr = getenv("EZ_APPKEY")) {
            this->appKey = string(envStr);
        }
        if(envStr = getenv("EZ_APPSECRET")) {
            this->appSecret = string(envStr);
        }
        if(envStr = getenv("EZ_VIDEO_DIR")) {
            this->videoDir = string(envStr);
        }

        if(envStr = getenv("EZ_BATCH_SIZE")) {
            this->numConcurrentDevs = stoi(string(envStr));
        }

        if(envStr = getenv("EZ_AMQP_ADDR")) {
            this->amqpConfig.amqpAddr = string(envStr);
        }
        if(envStr = getenv("EZ_REDIS_ADDR")) {
            this->redisAddr = string(envStr);
        }

        if(envStr = getenv("EZ_REDIS_PORT")) {
            this->redisPort = stoi(string(envStr));
        }

        if(envStr = getenv("EZ_APISRV_ADDR")) {
            this->apiSrvAddr = string(envStr);
        }

        if(envStr = getenv("EZ_UPLOAD_PROG_PATH")) {
            this->uploadProgPath = string(envStr);
            if(this->uploadProgPath.length() >= MAX_PATH_NUM_CHARS) {
                cerr << "invalid length of path of upload program" << endl;
                exit(1);
            }
        }
    }

    void toString()
    {
        cout << "\nENVCONFIG:\n\tAMQP ADDR: " << this->amqpConfig.amqpAddr
             <<"\n\tROUTING KEY: " <<this->amqpConfig.rtstopRouteKey
             <<"\n\tREDIS: " << this->redisAddr <<":" << this->redisPort
             <<"\n\tVIDEO DIR: " << this->videoDir
             <<"\n\tBATCH SIZE: " << this->numConcurrentDevs
             <<"\n\tAPPKEY: " << this->appKey
             <<"\n\tAPPSECRET: " << this->appSecret
             <<"\n\tAPI SRV ADDR: " << this->apiSrvAddr
             <<"\n\tUPLOAD PROG PATH: " << this->uploadProgPath <<endl;
    }
} EnvConfig;

typedef struct EZCallBackUserData {
    // video file handler
    ofstream *fout;
    // 0: download ended; otherwise: downloading
    int stat;
    // guard multi-thread file writing
    mutex m;
    // retried times on network failure etc. TODO: NOT IMPLEMENTED
    int numRetried;
    // downloadBytes
    unsigned long long bytesWritten;
} EZCallBackUserData;


typedef ST_ES_RECORD_INFO * ST_ES_RECORD_INFO_PTR;

typedef struct EZJobDetail {
    string devsn;
    string devcode;
    ST_ES_RECORD_INFO_PTR *cloudVideos;
    ST_ES_RECORD_INFO_PTR *sdVideos;
    int fileNumCloud;
    int fileNumSD;
} EZJobDetail;

template <typename T>
class safe_vector {
private:
    vector<T> vec = vector<T> {};
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
        if (vec.size() > 0) {
            ret = vec.back();
            vec.pop_back();
        }

        return ret;
    }

    size_t size()
    {
        lock_guard<std::mutex> guard(_m);
        return vec.size();
    }

    vector<T> &get()
    {
        lock_guard<std::mutex> guard(_m);
        return vec;
    }
};

template <typename T>
mutex safe_vector<T>::_m;

namespace myutils {
using namespace Poco::Net;
using namespace Poco;

Context::Ptr context = new Poco::Net::Context(
    Context::CLIENT_USE, "", "", "",
    Context::VERIFY_NONE, 9, false,
    "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");

string HTTPPostRequest(string url, string body, map<string,string> headers)
{
    try {

        // prepare session
        URI uri(url);
        HTTPSClientSession session(uri.getHost(), uri.getPort(), context);

        // prepare path
        string path(uri.getPathAndQuery());
        if (path.empty()) path = "/";

        // send request
        HTTPRequest req(HTTPRequest::HTTP_POST, path, HTTPMessage::HTTP_1_1);
        req.setContentType("application/x-www-form-urlencoded");

        // Set headers here
        if(!headers.empty()) {
            for(map<string,string>::iterator it = headers.begin();
                    it != headers.end(); it++) {
                req.set(it->first, it->second);
            }

        }

        // Set the request body
        req.setContentLength( body.length() );

        // sends request, returns open stream
        std::ostream& os = session.sendRequest(req);
        os << body;  // sends the body
        //req.write(std::cout); // print out request

        // get response
        HTTPResponse res;
        cout << res.getStatus() << " " << res.getReason() << endl;

        istream &is = session.receiveResponse(res);
        stringstream ss;
        StreamCopier::copyStream(is, ss);

        return ss.str();
    }
    catch (Exception &ex) {
        cerr << ex.displayText() << endl;
        return "";
    }
}

// split strings
vector<string> split(const std::string& s, char delimiter)
{
   std::vector<std::string> tokens;
   std::string token;
   std::istringstream tokenStream(s);
   while (getline(tokenStream, token, delimiter))
   {
      tokens.push_back(token);
   }
   return tokens;
}

}

#endif