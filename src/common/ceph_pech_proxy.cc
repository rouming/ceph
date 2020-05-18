#include "include/mempool.h"
#include "include/utime.h"
#include "rados/objclass.h"
#include "common/deleter.h"

#include "objclass/objclass.h"

// mempool.cc

mempool::pool_t& mempool::get_pool(mempool::pool_index_t ix)
{
  // We rely on this array being initialized before any invocation of
  // this function, even if it is called by ctors in other compilation
  // units that are being initialized before this compilation unit.
  static mempool::pool_t table[num_pools];
  return table[ix];
}

void mempool::pool_t::adjust_count(ssize_t items, ssize_t bytes)
{
  shard_t *shard = pick_a_shard();
  shard->items += items;
  shard->bytes += bytes;
}

// assert.cc

namespace ceph {
  void __ceph_assert_fail(const char *assertion,
                          const char *file, int line,
                          const char *func)
  {
    abort();
  }

  void __ceph_assert_fail(const assert_data &ctx)
  {
    __ceph_assert_fail(ctx.assertion, ctx.file, ctx.line, ctx.function);
  }

  void __ceph_abort(const char *file, int line,
                    const char *func, const std::string& msg)
  {
    abort();
  }
};

struct ceph_iovec {
  struct iovec  *iovec;
  void (*release)(struct ceph_iovec *);
  unsigned long length;
  unsigned long nr_segs;
  int           refs;
};

static void ceph_iovec_get(struct ceph_iovec *vec)
{
  vec->refs++;
}

static void ceph_iovec_put(struct ceph_iovec *vec)
{
  if (!--vec->refs)
    vec->release(vec);
}

struct cls_iovec_deleter : public deleter::impl {
  struct ceph_iovec *vec;

  cls_iovec_deleter(struct ceph_iovec *vec_) :
    deleter::impl(deleter()),
    vec(vec_) {
    ceph_iovec_get(vec);
  }

  ~cls_iovec_deleter() override {
    ceph_iovec_put(vec);
  }
};

static int cls_bl_from_iovec(bufferlist &bl, struct ceph_iovec *vec)
{
  size_t length = vec->length;
  int ret = 0;

  try {
    /* Prepare in iovec */
    for (uint32_t i = 0; length && i < vec->nr_segs; i++) {
      const struct iovec *iov = &vec->iovec[i];
      size_t len = min(iov->iov_len, length);

      bl.append(buffer::claim_buffer(len, (char *)iov->iov_base,
                deleter(new cls_iovec_deleter(vec))));

      length -= len;
    }
  } catch (std::bad_alloc &) {
    ret = -ENOMEM;
  }

  return ret;
}

struct ceph_cls_call_ctx {
  struct ceph_cls_callback_ops
                         *ops;
  char                   *in;
  struct ceph_iovec      **out;
  unsigned int           in_len;
};

struct ceph_cls_callback_ops {
  int (*execute_op)(struct ceph_cls_call_ctx *ctx,
                    struct ceph_osd_op *op,
                    struct ceph_iovec *in,
                    struct ceph_iovec **out);
  int (*describe_req)(struct ceph_cls_call_ctx *ctx,
                      struct ceph_cls_req_desc *desc);
};

struct ceph_cls_proxy_call_ctx {
  struct ceph_cls_call_ctx *ctx;
  union {
    cls_method_call_t      c_method;
    cls_method_cxx_call_t  cxx_method;
  };
  unsigned int             is_cxx;  /* 1 if method is registered as CXX */
};

struct ceph_cls_req_desc {
  struct ceph_entity_inst source;
  uint64_t                peer_features;
};

struct ceph_bufferlist : public ceph_iovec {
  std::vector<struct iovec> iovecs;
  bufferlist bl;

  static void release_iovec(struct ceph_iovec *iovec) {
    delete static_cast<struct ceph_bufferlist *>(iovec);
  }

  void bl_to_iovec()
  {
    const bufferlist::buffers_t &bufs = bl.buffers();
    unsigned int i = 0;

    iovecs.resize(bl.get_num_buffers() ?: 1);

    for (auto iter = bufs.begin(); iter != bufs.end(); ++iter, i++) {
      struct iovec *iov = &iovecs[i];

      iov->iov_base = (void *)iter->c_str();
      iov->iov_len = iter->length();
    }

    ceph_iovec::iovec   = &iovecs[0];
    ceph_iovec::release = release_iovec;
    ceph_iovec::nr_segs = bl.get_num_buffers();
    ceph_iovec::length  = bl.length();
    ceph_iovec::refs    = 1;
  }
};

static int cls_proxy_cxx_method_call(struct ceph_cls_proxy_call_ctx *proxy_ctx)
{
  struct ceph_cls_call_ctx *ctx = proxy_ctx->ctx;
  ceph_bufferlist *out = NULL;
  int ret;

  try {
    bufferlist in;

    in.append(buffer::create_static(ctx->in_len, ctx->in));

    out = new ceph_bufferlist;
    ret = proxy_ctx->cxx_method(ctx, &in, &out->bl);
    if (ret || !out->bl.length()) {
      delete out;
      out = NULL;
    } else {
      out->bl_to_iovec();
    }

    *ctx->out = out;

  } catch (std::bad_alloc &) {
    delete out;
    ret = -ENOMEM;
  } catch (...) {
    delete out;
    /* Argh */
    ret = -EINVAL;
  }

  return ret;
}

struct cls_buf_deleter : public deleter::impl {
  void *ptr;

  cls_buf_deleter(void *ptr_) :
    deleter::impl(deleter()),
    ptr(ptr_) {}

  ~cls_buf_deleter() override {
    cls_free(ptr);
  }
};

static int cls_proxy_c_method_call(struct ceph_cls_proxy_call_ctx *proxy_ctx)
{
  struct ceph_cls_call_ctx *ctx = proxy_ctx->ctx;
  ceph_bufferlist *out = NULL;
  char *outdata = NULL;
  int outdata_len = 0;
  int ret;

  ret = proxy_ctx->c_method(ctx, ctx->in, ctx->in_len,
                            &outdata, &outdata_len);
  ceph_assert(!(ret && outdata));

  if (!ret && outdata) {
    try {
      out = new ceph_bufferlist;
      out->bl.append(buffer::claim_buffer(outdata_len, outdata,
                     deleter(new cls_buf_deleter(outdata))));
      out->bl_to_iovec();

      *ctx->out = out;

    } catch (std::bad_alloc &) {
      cls_free(outdata);
      delete out;
      ret = -ENOMEM;
    }
  }

  return ret;
}

/**
 * cls_proxy_method_call() - called by Pech OSD
 */
extern "C"
int cls_proxy_method_call(struct ceph_cls_proxy_call_ctx *proxy_ctx)
{
  if (proxy_ctx->is_cxx)
    return cls_proxy_cxx_method_call(proxy_ctx);

  return cls_proxy_c_method_call(proxy_ctx);
}

static int cls_cxx_execute_op(cls_method_context_t hctx,
                              struct ceph_osd_op *op,
                              bufferlist &in_bl, bufferlist &out_bl)
{
  struct ceph_iovec *out_vec = NULL;
  struct ceph_cls_call_ctx *ctx;
  ceph_bufferlist *in = NULL;
  int ret;

  ctx = reinterpret_cast<ceph_cls_call_ctx *>(hctx);

  try {
    in = new ceph_bufferlist;
    in->bl = std::move(in_bl);
    in->bl_to_iovec();
  } catch (std::bad_alloc &) {
    delete in;
    return -ENOMEM;
  }

  ret = ctx->ops->execute_op(ctx, op, in, &out_vec);
  ceph_iovec_put(in);

  if (ret)
    return ret;

  if (out_vec) {
    /* Claim out buffers */
    ret = cls_bl_from_iovec(out_bl, out_vec);
    ceph_iovec_put(out_vec);
  }

  return ret;
}

// rados objclass API

void *cls_alloc(size_t size)
{
  return malloc(size);
}

void cls_free(void *p)
{
  free(p);
}

enum {
  CEPH_CLS_METHOD_CXX = 1<<16 /* internally handled by Pech class_loader.c */
};

int cls_register_cxx_method(cls_handle_t hclass, const char *mname,
                            int flags, cls_method_cxx_call_t func,
                            cls_method_handle_t *handle)
{
  return cls_register_method(hclass, mname, flags | CEPH_CLS_METHOD_CXX,
                             (cls_method_call_t)func, handle);
}

// borrowed from crimson/osd/objclass.cc

int cls_getxattr(cls_method_context_t hctx,
                 const char *name,
                 char **outdata,
                 int *outdatalen)
{
  ceph_abort();
  return 0;
}

int cls_setxattr(cls_method_context_t hctx,
                 const char *name,
                 const char *value,
                 int val_len)
{
  ceph_abort();
  return 0;
}

int cls_read(cls_method_context_t hctx,
             int ofs, int len,
             char **outdata,
             int *outdatalen)
{
  ceph_abort();
  return 0;
}

int cls_get_request_origin(cls_method_context_t hctx, entity_inst_t *origin)
{
  struct ceph_cls_req_desc desc;
  struct ceph_cls_call_ctx *ctx;
  int ret;

  ctx = reinterpret_cast<ceph_cls_call_ctx *>(hctx);
  ret = ctx->ops->describe_req(ctx, &desc);
  if (ret)
    return ret;

  *origin = desc.source;

  return 0;
}

int cls_cxx_create(cls_method_context_t hctx, const bool exclusive)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;

  op.op = CEPH_OSD_OP_CREATE;
  op.flags = exclusive ? CEPH_OSD_OP_FLAG_EXCL : 0;

  return cls_cxx_execute_op(hctx, &op, in, out);
}

int cls_cxx_remove(cls_method_context_t hctx)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;

  op.op = CEPH_OSD_OP_DELETE;

  return cls_cxx_execute_op(hctx, &op, in, out);
}

static int cls_cxx_stat3(cls_method_context_t hctx, uint64_t *size,
                         time_t *mtime, ceph::real_time *rtime)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;
  int ret;

  ceph_assert(!(mtime && rtime));

  op.op = CEPH_OSD_OP_STAT;

  ret = cls_cxx_execute_op(hctx, &op, in, out);
  if (ret)
    return ret;

  real_time rt;
  utime_t ut;
  uint64_t s;
  try {
    auto iter = out.cbegin();
    decode(s, iter);
    if (mtime)
      decode(ut, iter);
    else if (rtime)
      decode(rt, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  if (size) {
    *size = s;
  }
  if (mtime) {
    *mtime = ut.sec();
  } else if (rtime) {
    *rtime = rt;
  }

  return 0;
}

int cls_cxx_stat2(cls_method_context_t hctx,
                  uint64_t *size, ceph::real_time *rtime)
{
  return cls_cxx_stat3(hctx, size, NULL, rtime);
}

int cls_cxx_stat(cls_method_context_t hctx,
                 uint64_t *size, time_t *mtime)
{
  return cls_cxx_stat3(hctx, size, mtime, NULL);
}

int cls_cxx_read2(cls_method_context_t hctx,
                  int ofs, int len,
                  bufferlist *out,
                  uint32_t op_flags)
{
  struct ceph_osd_op op = {};
  bufferlist in;
  int ret;

  op.op = CEPH_OSD_OP_SYNC_READ;
  op.extent.offset = ofs;
  op.extent.length = len;
  op.flags = op_flags;

  ret = cls_cxx_execute_op(hctx, &op, in, *out);
  if (ret)
    return ret;

  return out->length();
}

int cls_cxx_read(cls_method_context_t hctx, int ofs, int len, bufferlist *out)
{
  return cls_cxx_read2(hctx, ofs, len, out, 0);
}

int cls_cxx_write2(cls_method_context_t hctx,
                   int ofs, int len,
                   bufferlist *in,
                   uint32_t op_flags)
{
  struct ceph_osd_op op = {};
  bufferlist out;

  op.op = CEPH_OSD_OP_WRITE;
  op.extent.offset = ofs;
  op.extent.length = len;
  op.flags = op_flags;

  return cls_cxx_execute_op(hctx, &op, *in, out);
}

int cls_cxx_write(cls_method_context_t hctx, int ofs, int len, bufferlist *in)
{
  return cls_cxx_write2(hctx, ofs, len, in, 0);
}

int cls_cxx_write_full(cls_method_context_t hctx, bufferlist * const in)
{
  struct ceph_osd_op op = {};
  bufferlist out;

  op.op = CEPH_OSD_OP_WRITEFULL;
  op.extent.offset = 0;
  op.extent.length = in->length();

  return cls_cxx_execute_op(hctx, &op, *in, out);
}

int cls_cxx_replace(cls_method_context_t hctx,
                    int ofs, int len,
                    bufferlist *in)
{
  {
    struct ceph_osd_op op = {};
    bufferlist in, out;
    int ret;

    op.op = CEPH_OSD_OP_TRUNCATE;
    op.extent.offset = 0;
    op.extent.length = 0;

    ret = cls_cxx_execute_op(hctx, &op, in, out);
    if (ret)
      return ret;
  }

  {
    struct ceph_osd_op op = {};
    bufferlist out;

    op.op = CEPH_OSD_OP_WRITE;
    op.extent.offset = ofs;
    op.extent.length = len;

    return cls_cxx_execute_op(hctx, &op, *in, out);
  }
}

int cls_cxx_truncate(cls_method_context_t hctx, int ofs)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;

  op.op = CEPH_OSD_OP_TRUNCATE;
  op.extent.offset = ofs;
  op.extent.length = 0;

  return cls_cxx_execute_op(hctx, &op, in, out);
}

int cls_cxx_getxattr(cls_method_context_t hctx,
                     const char *name, bufferlist *out)
{
  struct ceph_osd_op op = {};
  bufferlist in;

  op.op = CEPH_OSD_OP_GETXATTR;
  op.xattr.name_len = strlen(name);

  in.append(name, op.xattr.name_len);

  return cls_cxx_execute_op(hctx, &op, in, *out);
}

int cls_cxx_getxattrs(cls_method_context_t hctx,
                      map<string, bufferlist> *attrset)
{
  return 0;
}

int cls_cxx_setxattr(cls_method_context_t hctx,
                     const char *name, bufferlist *inbl)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;

  op.op = CEPH_OSD_OP_SETXATTR;
  op.xattr.name_len = std::strlen(name);
  op.xattr.value_len = inbl->length();

  in.append(name, op.xattr.name_len);
  in.append(*inbl);

  return cls_cxx_execute_op(hctx, &op, in, out);
}

int cls_cxx_snap_revert(cls_method_context_t hctx, snapid_t snapid)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;

  op.op = CEPH_OSD_OP_ROLLBACK;
  op.snap.snapid = snapid;

  return cls_cxx_execute_op(hctx, &op, in, out);
}

int cls_cxx_map_get_all_vals(cls_method_context_t hctx,
                             map<string, bufferlist>* vals,
                             bool *more)
{
  return 0;
}

int cls_cxx_map_get_keys(cls_method_context_t hctx,
                         const std::string& start_obj,
                         const uint64_t max_to_get,
                         std::set<std::string>* const keys,
                         bool* const more)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;
  int ret;

  op.op = CEPH_OSD_OP_OMAPGETKEYS;
  encode(start_obj, in);
  encode(max_to_get, in);

  ret = cls_cxx_execute_op(hctx, &op, in, out);
  if (ret)
    return ret;

  try {
    auto iter = out.cbegin();
    decode(*keys, iter);
    decode(*more, iter);
  } catch (buffer::error&) {
    return -EIO;
  }

  return keys->size();
}

int cls_cxx_map_get_vals(cls_method_context_t hctx,
                         const std::string& start_obj,
                         const std::string& filter_prefix,
                         const uint64_t max_to_get,
                         std::map<std::string, ceph::bufferlist> *vals,
                         bool* const more)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;
  int ret;

  op.op = CEPH_OSD_OP_OMAPGETVALS;
  encode(start_obj, in);
  encode(max_to_get, in);
  encode(filter_prefix, in);

  ret = cls_cxx_execute_op(hctx, &op, in, out);
  if (ret)
    return ret;

  try {
    auto iter = out.cbegin();
    decode(*vals, iter);
    decode(*more, iter);
  } catch (buffer::error&) {
    return -EIO;
  }

  return vals->size();
}

int cls_cxx_map_read_header(cls_method_context_t hctx, bufferlist *outbl)
{
  return 0;
}

int cls_cxx_map_get_val(cls_method_context_t hctx,
                        const string &key,
                        bufferlist *outbl)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;
  int ret;

  op.op = CEPH_OSD_OP_OMAPGETVALSBYKEYS;
  {
    std::set<std::string> k{key};
    encode(k, in);
  }

  ret = cls_cxx_execute_op(hctx, &op, in, out);
  if (ret)
    return ret;

  std::map<std::string, ceph::bufferlist> m;
  try {
    auto iter = out.cbegin();
    decode(m, iter);
  } catch (buffer::error&) {
    return -EIO;
  }
  if (auto iter = std::begin(m); iter != std::end(m)) {
    *outbl = std::move(iter->second);
    return 0;
  }

  return -ENOENT;
}

int cls_cxx_map_set_val(cls_method_context_t hctx,
                        const string &key,
                        bufferlist *inbl)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;

  op.op = CEPH_OSD_OP_OMAPSETVALS;
  {
    std::map<std::string, ceph::bufferlist> m;
    m[key] = *inbl;
    encode(m, in);
  }

  return cls_cxx_execute_op(hctx, &op, in, out);
}

int cls_cxx_map_set_vals(cls_method_context_t hctx,
                         const std::map<string, ceph::bufferlist> *map)
{
  struct ceph_osd_op op = {};
  bufferlist in, out;

  op.op = CEPH_OSD_OP_OMAPSETVALS;
  encode(*map, in);

  return cls_cxx_execute_op(hctx, &op, in, out);
}

int cls_cxx_map_clear(cls_method_context_t hctx)
{
  return 0;
}

int cls_cxx_map_write_header(cls_method_context_t hctx, bufferlist *inbl)
{
  return 0;
}

int cls_cxx_map_remove_range(cls_method_context_t hctx,
                             const std::string& key_begin,
                             const std::string& key_end)
{
  return 0;
}

int cls_cxx_map_remove_key(cls_method_context_t hctx, const string &key)
{
  return 0;
}

int cls_cxx_list_watchers(cls_method_context_t hctx,
                          obj_list_watch_response_t *watchers)
{
  return 0;
}

uint64_t cls_current_version(cls_method_context_t hctx)
{
  return 0;
}


int cls_current_subop_num(cls_method_context_t hctx)
{
  return 0;
}

uint64_t cls_get_features(cls_method_context_t hctx)
{
  return 0;
}

uint64_t cls_get_client_features(cls_method_context_t hctx)
{
  struct ceph_cls_req_desc desc;
  struct ceph_cls_call_ctx *ctx;
  int ret;

  ctx = reinterpret_cast<ceph_cls_call_ctx *>(hctx);
  ret = ctx->ops->describe_req(ctx, &desc);
  if (ret)
    return ret;

  return desc.peer_features;
}

ceph_release_t cls_get_required_osd_release(cls_method_context_t hctx)
{
  // FIXME
  return ceph_release_t::nautilus;
}

ceph_release_t cls_get_min_compatible_client(cls_method_context_t hctx)
{
  // FIXME
  return ceph_release_t::nautilus;
}

int cls_get_snapset_seq(cls_method_context_t hctx, uint64_t *snap_seq)
{
  return 0;
}

int cls_cxx_chunk_write_and_set(cls_method_context_t hctx,
                                int ofs,
                                int len,
                                bufferlist *write_inbl,
                                uint32_t op_flags,
                                bufferlist *set_inbl,
                                int set_len)
{
  return 0;
}

bool cls_has_chunk(cls_method_context_t hctx, string fp_oid)
{
  return 0;
}

uint64_t cls_get_osd_min_alloc_size(cls_method_context_t hctx)
{
  // FIXME
  return 4096;
}
