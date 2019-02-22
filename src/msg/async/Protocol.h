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
    encoded(encoded_),
    tag(CEPH_MSGR_TAG_MSG) {
  }

  QueuedMessage(char tag_, const void *data_, unsigned int len_) :
    msg(nullptr),
    length(len_),
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
  bool lossy;
  AsyncMessenger *messenger;
  AsyncConnection *connection;
  uint64_t out_seq;

  /*
   * Before changing iovec capacity, please, do appropriate performance
   * measurements for all possible sets of block sizes. The value below
   * was not chosen by chance.
   */
  std::array<struct iovec, 64> iovec;
  decltype(iovec)::iterator iovec_beg_it;
  decltype(iovec)::iterator iovec_end_it;

  std::list<QueuedMessage> msgs;
  decltype(msgs)::iterator msgs_beg_it;
  decltype(msgs)::iterator msgs_end_it;
  unsigned int msg_beg_pos;
  unsigned int msg_end_pos;

  WriteQueue(bool lossy_, AsyncMessenger *messenger_,
	     AsyncConnection *connection_) :
    lossy(lossy_),
    messenger(messenger_),
    connection(connection_),
    out_seq(0),
    iovec_beg_it(iovec.begin()),
    iovec_end_it(iovec.begin()),
    msgs_beg_it(msgs.end()),
    msgs_end_it(msgs.end()),
    msg_beg_pos(0),
    msg_end_pos(0)
  {}

  bool is_iovec_empty() const {
    return iovec_beg_it == iovec_end_it;
  }

  bool has_msgs_to_send() const {
    return msgs_end_it != msgs.end();
  }

  void enqueue(std::list<QueuedMessage> &list) {
    std::list<QueuedMessage>::iterator beg_it;

    ceph_assert(!list.empty());
    beg_it = list.begin();
    msgs.splice(msgs.end(), list);
    if (msgs_beg_it == msgs.end())
      msgs_beg_it = msgs.begin();
    if (msgs_end_it == msgs.end())
      msgs_end_it = beg_it;
  }

  void enqueue(char tag, void *data, size_t len) {
    msgs.emplace_back(tag, data, len);
    if (msgs_beg_it == msgs.end())
      msgs_beg_it = msgs.begin();
    if (msgs_end_it == msgs.end())
      msgs_end_it = --msgs.end();
  }

  bufferlist::buffers_t::const_iterator
  find_buf(const bufferlist::buffers_t &bufs,
	   unsigned int pos);

  struct iovec *fillin_iovec_from_mem(void *mem,
				      unsigned int &pos,
				      unsigned int beg,
				      unsigned int end);

  struct iovec *fillin_iovec_from_bufs(const bufferlist::buffers_t &bufs,
				       unsigned int &pos,
				       unsigned int beg);

  void prepare_for_write(QueuedMessage *qmsg);
  void fillin_iovec();
  void advance(unsigned int size);
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

  //XXX
  inline bool known_priority(unsigned int priority) const {
    return (priority == CEPH_MSG_PRIO_LOW ||
	    priority == CEPH_MSG_PRIO_DEFAULT ||
	    priority == CEPH_MSG_PRIO_HIGH ||
	    priority == CEPH_MSG_PRIO_HIGHEST);
  }
};

#endif /* _MSG_ASYNC_PROTOCOL_ */
