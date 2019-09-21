#include <cpp_redis/cpp_redis>
#include <iostream>
#include <spdlog/spdlog.h>

using namespace std;

static cpp_redis::client redisClient;

int InitRedis()
{
    redisClient.connect("192.168.1.102", 6379, [](const std::string& host, std::size_t port, cpp_redis::client::connect_state status) {
        if (status == cpp_redis::client::connect_state::dropped) {
            spdlog::error("redis client disconnected from: {}:{} ", host , port);
        }
    }, 1000*2, -1, 1000*3);

    redisClient.sync_commit();
    return 0;
}

int RedisSAdd(string key, vector<string> val){
    auto f_ = redisClient.sadd(key, val);
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
    auto get_ = redisClient.smembers(key);
    redisClient.sync_commit();
    auto r = get_.get();
    if(r.is_array()) {
        auto arr = r.as_array();
        for(auto &i:arr) {
            ret.push_back(i.as_string());
        }
    }

    return ret;
}

int main(){
    InitRedis();

    string key = "test_sadd";
    vector<string> val = {"1", "2", "3"};
    int r = RedisSAdd(key, val);
    if(r == 0) {
        cout << "no change";
    }

    auto v = RedisSMembers(key);
    for(auto &i:v) {
        cout << i << ";";
    }

    return 0;
}