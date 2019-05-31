/*=============================================================================
#  Author:           blu (bruce.lu)
#  Email:            lzbgt@126.com 
#  FileName:         main.cpp
#  Description:      /
#  Version:          0.0.1
#  History:         
=============================================================================*/

#include <ESOpenStream.h>
#include <cstring>
#include <string>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <list>
#include <mutex>
#include <time.h>
#include "json.hpp"
#include "clipp.hpp"
#include "httplib.hpp"
#include <filesystem>

namespace fs = std::filesystem;
using namespace std;
using json = nlohmann::json;

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
    RTSTREAM,
    SERVER
} ACTION;

typedef struct CBUSERDATA
{
    ofstream *fout;
    int stat;
    // avoiding miltithreads racing writing to one file
    mutex m;
} CBUSERDATA;

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

    T *pop_back()
    {
        lock_guard<std::mutex> guard(_m);
        T *ret = NULL;
        if (vec.size() > 0)
        {
            ret = &vec.back();
            vec.pop_back();
        }
        return ret;
    }

    size_t size()
    {
        return vec.size();
    }

    vector<T>& get() {
        return vec;
    }
};
template<typename T>
mutex safe_vector<T>::_m;

template <typename T>
class safe_list
{
    private:
    list<T> _list = list<T>{};
    static mutex _m;

    public:
    list<T> &get(){
        return _list;
    }
};
template<typename T>
mutex safe_list<T>::_m;

int msgCb(HANDLE pHandle, int code, int eventType, void *pUser)
{
    cout << "=====> msg h: " << pHandle << " code: " << code << " evt: " << eventType << " pd: " << pUser << endl;
    CBUSERDATA *cbd = (CBUSERDATA *)pUser;
    if (code == ES_STREAM_CODE::ES_STREAM_CLIENT_RET_OVER)
    {
        cbd->stat = 0;
    }
    return 0;
}

// TODO: lock per file, avoding racing writes by multiple threads!
int dataCb(HANDLE pHandle, unsigned int dataType, unsigned char *buf, unsigned int buflen, void *pUser)
{
    CBUSERDATA *cbd = (CBUSERDATA *)pUser;
    if (ES_STREAM_TYPE::ES_STREAM_DATA == dataType)
    {
        cbd->m.lock();
        cbd->fout->write(reinterpret_cast<const char *>(buf), buflen);
        cbd->m.unlock();
    }
    else if (ES_STREAM_TYPE::ES_STREAM_END == dataType)
    {
        cbd->stat = 0;
    }

    return 0;
}
json search_records_json(string token, ST_ES_DEVICE_INFO &dev, string startTime, string endTime)
{
    int ret = 0;
    void *pOut = NULL;
    int length = 0;
    ES_RECORD_INFO ri = {"", "", 0, ""};
    std::strncpy(ri.szStartTime, startTime.c_str(), sizeof(ri.szStartTime));
    std::strncpy(ri.szStopTime, endTime.c_str(), sizeof(ri.szStopTime));

    cout << "seaarch json, start: " << startTime << ", token: " << token << endl;
    json j;
    ret = ESOpenSDK_SearchVideoRecord(token.c_str(), dev, ri, &pOut, &length);
    if (0 != ret)
    {
        j["code"] = 2;
        j["message"] = string("failed search record: ") + dev.szDevSerial + ", start:" + startTime + ", end: " + endTime;
        return j;
    }
    j = json::parse((char *)pOut);
    ESOpenSDK_FreeData(pOut);

    return j;
}

safe_vector<ES_RECORD_INFO *> *search_records(string token, ST_ES_DEVICE_INFO &dev, string startTime, string endTime)
{
    int ret = 0;
    void *pOut = NULL;
    int length = 0;
    safe_vector<ES_RECORD_INFO *> *recList = NULL;
    ES_RECORD_INFO ri = {"", "", 0, ""};
    std::strncpy(ri.szStartTime, startTime.c_str(), sizeof(ri.szStartTime));
    std::strncpy(ri.szStopTime, endTime.c_str(), sizeof(ri.szStopTime));

    ret = ESOpenSDK_SearchVideoRecord(token.c_str(), dev, ri, &pOut, &length);
    if (0 != ret)
    {
        return recList;
    }
    json j = json::parse((char *)pOut);
    ESOpenSDK_FreeData(pOut);

    if (j["dataSize"] != 0)
    {
        // populate recList
        recList = new safe_vector<ES_RECORD_INFO *>();
        for (auto &i : j["data"])
        {
            ES_RECORD_INFO *tmp = new ES_RECORD_INFO();
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
void get_records(string token, ST_ES_DEVICE_INFO &dev, safe_vector<ES_RECORD_INFO *> *recList, string dir)
{
    cout << "get records" << endl;
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
                    cout << "secs: " << secs << endl;
                    strftime(tmStr, sizeof(tmStr), "%Y%m%d%H%M%S", &tm1);
                    string filename = tmStr;
                    filename = dir + "/" + filename;
                    filename += string("_") + string(dev.szDevSerial) + "_" + to_string(secs) + ".mpg";
                    ofstream *fout = new ofstream();
                    fout->open(filename, ios_base::binary | ios_base::trunc);
                    cout << "filename: " << filename << endl;
                    CBUSERDATA cbd = {fout, 1};
                    ES_STREAM_CALLBACK scb = {msgCb, dataCb, (void *)&cbd};
                    HANDLE handle = NULL;
                    ret = ESOpenSDK_StartPlayBack(token.c_str(), dev, *rip, scb, handle);
                    if (0 != ret)
                    {
                        cout << "failed to playback, dev: " << dev.szDevSerial << ", code: " << dev.szSafeKey << " video: " << rip->szStartTime << " - " << rip->szStopTime << ", recType: " << rip->iRecType << endl;
                    }
                    else
                    {
                        while (cbd.stat == 1)
                        {
                            usleep(1000 * 1000 * 4);
                            cout << "snap for downloading to finish" << endl;
                        }
                        cbd.fout->flush();
                        cbd.fout->close();
                        cout << "file " << filename << "downloaded!"
                             << " looking for next record. remains: " << recList->size() << endl;
                        ESOpenSDK_StopPlayBack(handle);
                    }

                    delete cbd.fout;
                    // fetch next record
                }
            }
        });
    } // end for

    // wait for all threads
    for (int i = 0; i < 8; i++)
    {
        if (threads[i].joinable())
        {
            threads[i].join();
        }
    }
    cout << "all threads finished!" << endl;
    // delete threads;
}

// download realtime stream
void get_rtstream(string token, ST_ES_DEVICE_INFO &dev, string dir, int level)
{
    cout << "get realtime streaming" << endl;
    int ret = 0;
    char tmStr[15] = {0};

    ret = ESOpenSDK_SetVideoLevel(token.c_str(), dev, level);
    if (0 != ret)
    {
        cout << "failed to set stream quality level to: " << level << endl;
        cout << "get supported level values by sub-command: "
             << " info " << endl;
        return;
    }

    string filename = dir + "/";
    filename += string(dev.szDevSerial) + "_" + ".mpg";
    ofstream *fout = new ofstream();
    fout->open(filename, ios_base::binary | ios_base::trunc);
    cout << "filename: " << filename << endl;
    CBUSERDATA cbd = {fout, 1};
    ES_STREAM_CALLBACK scb = {msgCb, dataCb, (void *)&cbd};
    HANDLE handle = NULL;
    ret = ESOpenSDK_StartRealPlay(token.c_str(), dev, scb, handle);
    if (0 != ret)
    {
        cout << "failed to realtime play, dev: " << dev.szDevSerial << ", code: " << dev.szSafeKey << endl;
    }
    else
    {
        while (1)
        {
            usleep(1000 * 1000);
            // check cmd
            cbd.fout->flush();
            cbd.fout->close();
        }
        ESOpenSDK_StopRealPlay(handle);
    }

    delete cbd.fout;
    // fetch next record
    // delete threads;
}

void http_server()
{
// http server
#define mk_param(p) \
    {               \
#p, &p      \
    }
#define MAX_RUNNING_JOB 10

    using namespace httplib;
    typedef safe_vector<ES_RECORD_INFO *> *RECORD_P_VEC_PTR;
    typedef struct DOWNLOAD_REC_JOB
    {
        RECORD_P_VEC_PTR recordsPtrVecPtr;
        thread *thJob;
    } DOWNLOAD_REC_JOB;

    Server svr;

    svr.Get("/records_list", [](const Request &req, Response &res) {
        bool flag = true;
        string devsn, devkey, appkey, token, start, end;
        int chanId = 1;
        json ret;
        if (req.has_param("chanid"))
        {
            chanId = stoi(req.get_param_value("chanid"));
        }

        // assert params
        std::unordered_map<const char *, string *> params = {
            mk_param(devsn),
            mk_param(devkey),
            mk_param(appkey),
            mk_param(token),
            mk_param(start),
            mk_param(end)};
        for (auto &p : params) 
        {
            *(p.second) = req.get_param_value(p.first);
            if ((*(p.second)).empty())
            {
                ret["code"] = 1;
                ret["message"] = string("missing param: ") + p.first;
                res.set_content(ret.dump(), "application/json");
                return;
            }
        }

        // ready to go
        ST_ES_DEVICE_INFO dev = {"", 1, ""};
        std::strncpy(dev.szDevSerial, devsn.c_str(), sizeof(dev.szDevSerial));
        std::strncpy(dev.szSafeKey, devkey.c_str(), sizeof(dev.szSafeKey));
        dev.iDevChannelNo = chanId;
        ret = search_records_json(token, dev, start, end);
        res.set_content(ret.dump(), "application/json");
    });

    // track jobs
    safe_vector<DOWNLOAD_REC_JOB *> *recJobs = new safe_vector<DOWNLOAD_REC_JOB *>();
    svr.Get("/records_get", [&recJobs](const Request &req, Response &res) {
        bool flag = true;
        string devsn, devkey, appkey, token, start, end;
        int chanId = 1;
        json ret;
        if (req.has_param("chanid"))
        {
            chanId = stoi(req.get_param_value("chanid"));
        }
        // assert params
        std::unordered_map<const char *, string *> params = {
            mk_param(devsn),
            mk_param(devkey),
            mk_param(appkey),
            mk_param(token),
            mk_param(start),
            mk_param(end)};
        for (auto &p : params)
        {
            *(p.second) = req.get_param_value(p.first);
            if ((*(p.second)).empty())
            {
                ret["code"] = 1;
                ret["message"] = string("missing param: ") + p.first;
                res.set_content(ret.dump(), "application/json");
                return;
            }
        }

        // ready to go
        ST_ES_DEVICE_INFO dev = {"", 1, ""};
        std::strncpy(dev.szDevSerial, devsn.c_str(), sizeof(dev.szDevSerial));
        std::strncpy(dev.szSafeKey, devkey.c_str(), sizeof(dev.szSafeKey));
        dev.iDevChannelNo = chanId;
        ret = search_records_json(token, dev, start, end);
        RECORD_P_VEC_PTR recList = search_records(token, dev, start, end);
        DOWNLOAD_REC_JOB *recJobPtr = new DOWNLOAD_REC_JOB();
        recJobPtr->recordsPtrVecPtr = recList;
        recJobPtr->thJob = new thread([] {
            //get_records(token, dev, recList, DEFAULT_VIDEO_DIR);
        });

        recJobs->push_back(recJobPtr);

        ret["code"] = 0;
        ret["message"] = "video records downloading task is running on server";

        res.set_content(ret.dump(), "application/json");
    });

    svr.listen("0.0.0.0", 80);
}

int main(int argc, char *argv[])
{
    using namespace clipp;
    int ret = 0, chanId = 1;
    string appKey, appSecret, devSn, devCode, token, startTime, endTime;
    int numTcpThreads, numSslThreads;
    auto action = ACTION::NONE;

    auto cli = (command("info").set(action, ACTION::INFO) |
                    (command("records"),
                     (command("list").set(action, ACTION::RECORDS_LIST) |
                      command("get").set(action, ACTION::RECORDS_GET)),
                     value("chanId", chanId), value("startTime", startTime), value("endTime", endTime)) |
                    command("rtstream").set(action, ACTION::RTSTREAM),
                value("devSn", devSn), value("devCode", devCode), value("appKey", appKey), value("token", token)) |
               command("server").set(action, ACTION::SERVER);

    if (!parse(argc, argv, cli))
    {
        cout << "invalid argument, check if missing required fields" << endl;
        cout << make_man_page(cli, argv[0]);
        return 1;
    }
    const char *dir = getenv(ENV_VIDEO_DIR);

    //if (dir == NULL)
    //{
    dir = DEFAULT_VIDEO_DIR;
    //}
    if (!fs::exists(dir))
    {
        if (!fs::create_directory(dir))
        {
            cout << "can't create directory: " << dir << endl;
            exit(1);
        }
        fs::permissions(dir, fs::perms::all);
    }
    // init sdk
    ret = ESOpenSDK_Init(2, 1);
    ESOpenSDK_InitWithAppKey(appKey.c_str(), OPENADDR);
    if (0 != ret)
    {
        cout << "error init sdk" << endl;
        exit(1);
    }

    // dev info
    ST_ES_DEVICE_INFO dev = {"", 1, ""};
    std::strncpy(dev.szDevSerial, devSn.c_str(), sizeof(dev.szDevSerial));
    std::strncpy(dev.szSafeKey, devCode.c_str(), sizeof(dev.szSafeKey));
    dev.iDevChannelNo = chanId;

    // handle cmd
    switch (action)
    {
    case ACTION::INFO:
    {
        void *pOut = NULL;
        int length = 0;
        ret = ESOpenSDK_GetDevInfo(token.c_str(), dev, false, &pOut, &length);
        if (0 != ret)
        {
            cout << "cant get dev info" << endl;
            exit(1);
        }
        cout << (char *)pOut << endl;
        ESOpenSDK_FreeData(pOut);
        pOut = NULL;
        break;
    }
    case ACTION::RECORDS_LIST:
    case ACTION::RECORDS_GET:
    {
        // list
        safe_vector<ES_RECORD_INFO *> *recList = search_records(token, dev, startTime, endTime);
        if (recList->size() != 0)
        {
            int idx = 0;
            for (auto &r : recList->get())
            {
                idx++;
                cout << "\nindex " << idx << ": start: " << r->szStartTime << ", endTime: " << r->szStopTime << ", type: " << r->iRecType << endl;
            }
        }
        else
        {
            //
        }
        // no records
        if (action == ACTION::RECORDS_LIST)
        {
            cout << "delete all memory records";
            for (auto &r : recList->get())
            {
                delete r;
            }
            delete recList;
        }
        else
        {
            // get each
            get_records(token, dev, recList, dir);
        }
        break;
    }
    case ACTION::RTSTREAM:
    {
        // play to file
        break;
    }
    case ACTION::SERVER:
    {
        http_server();
        break;
    }
    }

    ESOpenSDK_Fini();

    return 0;
}