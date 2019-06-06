#ifndef __MY_RABITMQ_HPP__
#define __MY_RABITMQ_HPP__

#include <uv.h>
#include <condition_variable>
#include <amqpcpp.h>
#include <amqpcpp/libuv.h>
#include "../json.hpp"

using namespace std;

class EZAMQPHandler : public AMQP::LibUvHandler
{
private:
    condition_variable *cvDetach, *cvReady;
    int interval = 11;
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

    virtual uint16_t onNegotiate(AMQP::TcpConnection *connection, uint16_t interval)
    {
        interval = this->interval;

        // @todo
        //  set a timer in your event loop, and make sure that you call
        //  connection->heartbeat() every _interval_ seconds if no other
        //  instruction was sent in that period.

        // return the interval that we want to use
        return interval;
    }

    virtual void onAttached(AMQP::TcpConnection *connection)
    {
        cout << "onAttached" << endl;

    }

    virtual bool onSecured(AMQP::TcpConnection *connection, const SSL *ssl)
    {
        return AMQP::LibUvHandler::onSecured(connection, ssl);
    }

    virtual void onProperties(AMQP::TcpConnection *connection, const AMQP::Table &server, AMQP::Table &client)
    {
        AMQP::LibUvHandler::onProperties(connection, server, client);
        cout << "onProperties" << endl;
    }

    virtual void onReady(AMQP::TcpConnection *connection) 
    {
        cout << "onReady" << endl;
        cvReady->notify_all();
    }

    virtual void onClosed(AMQP::TcpConnection *connection) 
    {
        cout << "onClosed" << endl;
    }
    
    /**
     *  Method that is called when the TCP connection is lost or closed. This
     *  is always called if you have also received a call to onConnected().
     *  @param  connection  The TCP connection
     */
    virtual void onLost(AMQP::TcpConnection *connection) 
    {
        cout << "onLost" << endl;
    }

    /**
     *  Method that is called when the handler will no longer be notified.
     *  This is the last call to your handler, and it is typically used
     *  to clean up stuff.
     *  @param  connection      The connection that is being destructed
     */
    virtual void onDetached(AMQP::TcpConnection *connection)
    {
        cout << "onDetached" << endl;
        cvDetach->notify_all();
    }
    
public:
    /**
     *  Constructor
     *  @param  uv_loop
     */
    EZAMQPHandler(condition_variable *cvReady, condition_variable *cvDetach,uv_loop_t *loop) : AMQP::LibUvHandler(loop) {
        this->cvReady=cvReady;
        this->cvDetach = cvDetach;
    }

    /**
     *  Destructor
     */
    virtual ~EZAMQPHandler() = default;
};
#endif