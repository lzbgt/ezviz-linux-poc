#include <ESOpenStream.h>
#include <cstring>
#include <string>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <mutex>
#include <time.h>
#include "json.hpp"
#include "clipp.hpp"
#include "httplib.hpp"


using namespace std;
using json = nlohmann::json;

mutex g_pages_mutex;

#define OPENADDR "https://open.ys7.com"
#define ENV_VIDEO_DIR "YS_VIDEO_DIR"
#define DEFAULT_VIDEO_DIR "videos"

/*
token, sn, code
action:
    config token
    devinfo
    record
        list [s e t]
        get [s e t]
    play


- config
- download
*/

typedef enum ACTION
{
    NONE,
    INFO,
    RECORDS_LIST,
    RECORDS_GET,
    RTSTREAM
} ACTION;


typedef struct CBUSERDATA {
    ofstream * fout;
    int stat;
    // avoiding miltithreads racing writing to one file
    mutex m;
} CBUSERDATA;

template <typename T> class safe_vector{
    public:
    vector<T> vec= vector<T>{};
    public: 
    void push_back(const T & v) {
        lock_guard<std::mutex> guard(g_pages_mutex);
        vec.push_back(v);
    }

    T* pop_back() {
        lock_guard<std::mutex> guard(g_pages_mutex);
        T *ret = NULL;
        if(vec.size() > 0) {
            ret = &vec.back();
            vec.pop_back();
        }
        return ret;
    }

    size_t size(){
        return vec.size();
    }
};

int msgCb(HANDLE pHandle, int code, int eventType, void *pUser)
{
    cout << "=====> msg h: " << pHandle << " code: " << code << " evt: " << eventType << " pd: " << pUser << endl;
    CBUSERDATA *cbd = ( CBUSERDATA*)pUser;
    if (code == ES_STREAM_CODE::ES_STREAM_CLIENT_RET_OVER)
    {
        cbd->stat = 0;
    }
    return 0;
}

// TODO: lock per file, avoding racing writes by multiple threads!
int dataCb(HANDLE pHandle, unsigned int dataType, unsigned char *buf, unsigned int buflen, void *pUser)
{
    CBUSERDATA *cbd = ( CBUSERDATA*)pUser;
    if(ES_STREAM_TYPE::ES_STREAM_DATA == dataType) {
        cbd->m.lock();
        cbd->fout->write(reinterpret_cast<const char *>(buf), buflen);
        cbd->m.unlock();
    }else if(ES_STREAM_TYPE::ES_STREAM_END == dataType) {
        cbd->stat = 0;
    }

    return 0;
}

safe_vector<ES_RECORD_INFO *> * search_records(string token, ST_ES_DEVICE_INFO &dev, string startTime, string endTime){
    int ret = 0;
    void *pOut = NULL;
    int length = 0;
    safe_vector<ES_RECORD_INFO*> *recList= NULL;
    ES_RECORD_INFO ri = {"", "", 0, ""};
    std::strncpy(ri.szStartTime, startTime.c_str(), sizeof(ri.szStartTime));
    std::strncpy(ri.szStopTime, endTime.c_str(), sizeof(ri.szStopTime));

    ret = ESOpenSDK_SearchVideoRecord(token.c_str(), dev, ri, &pOut, &length);
    if(0 != ret) {
        return recList;
    }
    json j = json::parse((char *)pOut);
    ESOpenSDK_FreeData(pOut);

    if(j["dataSize"] != 0) {
        // populate recList
        recList = new safe_vector<ES_RECORD_INFO*>();
        for(auto &i : j["data"]) {
            ES_RECORD_INFO* tmp = new ES_RECORD_INFO();
            tmp->iRecType = i["recType"];
            tmp->szDownloadPath[0] = 0;
            std::strncpy(tmp->szStartTime, i["beginTime"].get<string>().c_str(), sizeof(tmp->szStartTime));
            std::strncpy(tmp->szStopTime, i["endTime"].get<string>().c_str(), sizeof(tmp->szStopTime));
            recList->push_back(tmp);
        }
    }

    return recList;
}

// download records
void get_records(string token, ST_ES_DEVICE_INFO& dev, safe_vector<ES_RECORD_INFO *> * recList, string dir) {
    cout <<"get records" << endl;
    thread *threads = new thread[8];
    for (int i = 0; i < 8; i++)
    {
        threads[i] = thread([&] {
            while (recList->size() > 0)
            {
                // download one record
                ES_RECORD_INFO **p = recList->pop_back();
                if (p != NULL)
                {
                    int ret = 0;
                    char tmStr[15] = {0};
                    ES_RECORD_INFO *rip = *p;
                    tm tm1 = {}, tm2 = {};
                    strptime(rip->szStartTime, "%Y-%m-%d %H:%M:%S", &tm1);
                    strptime(rip->szStopTime, "%Y-%m-%d %H:%M:%S", &tm2);
                    time_t t1 = mktime(&tm1), t2 = mktime(&tm2);
                    int secs = difftime(t2, t1);
                    cout <<"secs: " << secs<< endl;
                    strftime(tmStr, sizeof(tmStr), "%Y%m%d%H%M%S", &tm1);
                    string filename = tmStr;
                    filename = dir + "/" + filename;
                    filename += string("_") + string(dev.szDevSerial) + "_" + to_string(secs) + ".mpg";
                    ofstream *fout = new ofstream();
                    fout->open(filename, ios_base::binary|ios_base::trunc);
                    cout << "filename: " << filename<< endl;
                    CBUSERDATA cbd = {fout, 1};
                    ES_STREAM_CALLBACK scb = {msgCb, dataCb, (void *) &cbd};
                    HANDLE handle = NULL;
                    ret = ESOpenSDK_StartPlayBack(token.c_str(), dev, *rip, scb, handle);
                    while(cbd.stat == 1) {
                        usleep(1000*1000*4);
                        cout << "snap for downloading to finish" << endl;
                    }
                    cbd.fout->flush();
                    cbd.fout->close();
                    cout << "file " << filename << "downloaded!" << " looking for next record. remains: "<< recList->size() <<endl;
                    delete cbd.fout;
                    // fetch next record
                }
            }
        });
    }// end for

    // wait for all threads
    for(int i = 0; i < 8; i++) {
        if(threads[i].joinable()) {
            threads[i].join();
        }
    }
    cout << "all threads finished!" << endl;
    // delete threads;
}

int main(int argc, char *argv[])
{
    using namespace clipp;
    int ret = 0, chanId=1;
    string appKey, appSecret, devSn, devCode, token, startTime, endTime;
    int numTcpThreads, numSslThreads;
    auto action = ACTION::NONE;

    auto cli = (command("info").set(action, ACTION::INFO) |
                (command("records"),
                      (command("list").set(action, ACTION::RECORDS_LIST) |
                       command("get").set(action, ACTION::RECORDS_GET)), value("chanId", chanId), value("startTime", startTime), value("endTime",endTime) ) |
                command("rtstream").set(action, ACTION::RTSTREAM),
                value("devSn", devSn), value("devCode", devCode), value("appKey", appKey),value("token", token));

    if (!parse(argc, argv, cli))
    {
        cout << "invalid argument, check if missing required fields" << endl;
        cout << make_man_page(cli, argv[0]);
        return 1;
    }
    const char * dir = getenv(ENV_VIDEO_DIR);

    if(dir == NULL) {
        dir = DEFAULT_VIDEO_DIR;
    }

    if (mkdir(dir, 0777) != 0)
    {
        cout << "can't ceate video directory: " << dir << endl;
        return 1;
    }
    // init sdk
    ret = ESOpenSDK_Init(2, 1);
    ESOpenSDK_InitWithAppKey(appKey.c_str(), OPENADDR);
    if(0 != ret) {
        cout<< "error init sdk" << endl;
        exit(1);
    }

    // dev info
    ST_ES_DEVICE_INFO dev = {"", 1, ""};
    std::strncpy(dev.szDevSerial, devSn.c_str(), sizeof(dev.szDevSerial));
    std::strncpy(dev.szSafeKey, devCode.c_str(), sizeof(dev.szSafeKey));
    dev.iDevChannelNo = chanId;

    // handle cmd
    switch(action) {
        case ACTION::INFO: {
            void *pOut = NULL;
            int length = 0;
            ret = ESOpenSDK_GetDevInfo(token.c_str(), dev, false, &pOut, &length);
            if(0 != ret) {
                cout<< "cant get dev info" << endl;
                exit(1);
            }
            cout << (char *)pOut << endl;
            ESOpenSDK_FreeData(pOut);
            pOut = NULL;
            break;
        }
        case ACTION::RECORDS_LIST:
        case ACTION::RECORDS_GET: {
            // list
            safe_vector<ES_RECORD_INFO*> *recList = search_records(token, dev, startTime, endTime);
            if(recList->size() != 0) {
                int idx = 0;
                for(auto &r:recList->vec) {
                    idx++;
                    cout << "\nindex " << idx << ": start: " << r->szStartTime << ", endTime: " << r->szStopTime << ", type: " << r->iRecType << endl;
                }
            }else{
                //
            }
            // no records
            if(action == ACTION::RECORDS_LIST){
                cout << "delete all memory records";
                for(auto &r:recList->vec) {
                    delete r;
                }
                delete recList;
            }else{
                // get each
                get_records(token, dev, recList, dir);

            }
            break;
        }
        case ACTION::RTSTREAM: {
            // play to file
            break;
        }
    }

    ESOpenSDK_Fini();
    return 0;
}