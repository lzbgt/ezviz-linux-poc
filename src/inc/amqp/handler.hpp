#ifndef __MY_RABITMQ_HPP__
#define __MY_RABITMQ_HPP__

#include <uv.h>
#include <amqpcpp.h>
#include <amqpcpp/libuv.h>
#include "../json.hpp"

using namespace std;

class EZAMQPHandler : public AMQP::LibUvHandler
{
private:

    
    /**
     *  Method that is called when a connection error occurs
     *  @param  connection
     *  @param  message
     */
    virtual void onError(AMQP::TcpConnection *connection, const char *message) override
    {
        std::cout << "error: " << message << std::endl;
    }

    /**
     *  Method that is called when the TCP connection ends up in a connected state
     *  @param  connection  The TCP connection
     */
    virtual void onConnected(AMQP::TcpConnection *connection) override 
    {
        std::cout << "connected" << std::endl;
    }
    
public:
    /**
     *  Constructor
     *  @param  uv_loop
     */
    EZAMQPHandler(uv_loop_t *loop) : AMQP::LibUvHandler(loop) {}

    /**
     *  Destructor
     */
    virtual ~EZAMQPHandler() = default;
};
#endif