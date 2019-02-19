// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _MSG_ASYNC_PROTOCOL_
#define _MSG_ASYNC_PROTOCOL_

#include <list>
#include <map>

#include "AsyncConnection.h"
#include "AsyncMessenger.h"
#include "include/buffer.h"
#include "include/msgr.h"

/*
 * Continuation Helper Classes
 */

#include <memory>
#include <tuple>

template <class C>
class Ct {
public:
  virtual ~Ct() {}
  virtual Ct<C> *call(C *foo) const = 0;
};

template <class C, typename... Args>
class CtFun : public Ct<C> {
private:
  using fn = Ct<C> *(C::*)(Args...);
  fn _f;
  std::tuple<Args...> _params;

  template <std::size_t... Is>
  inline Ct<C> *_call(C *foo, std::index_sequence<Is...>) const {
    return (foo->*_f)(std::get<Is>(_params)...);
  }

public:
  CtFun(fn f) : _f(f) {}

  inline void setParams(Args... args) { _params = std::make_tuple(args...); }
  inline Ct<C> *call(C *foo) const override {
    return _call(foo, std::index_sequence_for<Args...>());
  }
};

#define CONTINUATION_DECL(C, F, ...)                    \
  std::unique_ptr<CtFun<C, ##__VA_ARGS__>> F##_cont_ =  \
      std::make_unique<CtFun<C, ##__VA_ARGS__>>(&C::F); \
  CtFun<C, ##__VA_ARGS__> *F##_cont = F##_cont_.get()

#define CONTINUATION_PARAM(V, C, ...) CtFun<C, ##__VA_ARGS__> *V##_cont

#define CONTINUATION(F) F##_cont
#define CONTINUE(F, ...) F##_cont->setParams(__VA_ARGS__), F##_cont

#define CONTINUATION_RUN(CT)                                      \
  {                                                               \
    Ct<std::remove_reference<decltype(*this)>::type> *_cont = CT; \
    while (_cont) {                                               \
      _cont = _cont->call(this);                                  \
    }                                                             \
  }

#define READ_HANDLER_CONTINUATION_DECL(C, F) \
  CONTINUATION_DECL(C, F, char *, int)
#define WRITE_HANDLER_CONTINUATION_DECL(C, F) CONTINUATION_DECL(C, F, int)

//////////////////////////////////////////////////////////////////////

struct QueuedMessage {
  Message *msg;
  unsigned int length;
  unsigned int pos;
  bool encoded;
  char tag;
  union {
    ceph_le64 s;
    struct ceph_timespec ts;
    ceph_msg_footer_old old_footer;
  } static_payload;  /* 13 bytes */

  QueuedMessage(Message *msg_, bool encoded_) :
    msg(msg_),
    length(0),
    pos(0),
    encoded(encoded_),
    tag(0) {
  }

  QueuedMessage(char tag_, const void *data_, unsigned int len_) :
    msg(nullptr),
    length(len_),
    pos(0),
    encoded(true),
    tag(tag_) {
    ceph_assert(len_ <= sizeof(static_payload));
    if (len_)
      memcpy(&static_payload, data_, len_);
  }

  ~QueuedMessage() {
    if (msg)
      msg->put();
  }
};

struct WriteQueue {
  AsyncMessenger *messenger;
  AsyncConnection *connection;
  std::array<struct iovec, IOV_MAX> iovec;
  decltype(iovec)::iterator iovec_pos;
  decltype(iovec)::iterator iovec_end;

  std::list<QueuedMessage> msgs;
  decltype(msgs)::iterator tosend_it;

  WriteQueue(AsyncMessenger *messenger_, AsyncConnection *connection_) :
    messenger(messenger_),
    connection(connection_),
    iovec_pos(iovec.begin()),
    iovec_end(iovec.begin())
  {}

  void enqueue(std::list<QueuedMessage> &list) {
    msgs.splice(msgs.end(), list);
  }

  void enqueue(char tag, void *data, size_t len) {
    msgs.emplace_back(tag, data, len);
  }
};

class Protocol {
public:
  const int proto_type;
protected:
  AsyncConnection *connection;
  AsyncMessenger *messenger;
  CephContext *cct;
  WriteQueue wqueue;
public:
  std::shared_ptr<AuthConnectionMeta> auth_meta;

public:
  Protocol(int type, AsyncConnection *connection);
  virtual ~Protocol();

  // prepare protocol for connecting to peer
  virtual void connect() = 0;
  // prepare protocol for accepting peer connections
  virtual void accept() = 0;
  // true -> protocol is ready for sending messages
  virtual bool is_connected() = 0;
  // stop connection
  virtual void stop() = 0;
  // signal and handle connection failure
  virtual void fault() = 0;
  // send message
  virtual void send_message(Message *m) = 0;
  // send keepalive
  virtual void send_keepalive() = 0;

  virtual void read_event() = 0;
  virtual void write_event() = 0;
  virtual bool is_queued() = 0;
};

#endif /* _MSG_ASYNC_PROTOCOL_ */
