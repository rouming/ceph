// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/errno.h"
#include <fcntl.h>
#include "EventEpoll.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "EpollDriver."

#define BUILD_BUG_ON(condition) ((void )sizeof(char [1 - 2*!!(condition)]))
#define READ_ONCE(v) (*(volatile decltype(v)*)&(v))

#define USE_USERPOLL
#define SPINS 100

static inline long epoll_create2(int flags, size_t size)
{
       return syscall(336, flags, size);
}

__attribute__((unused))
static void uepoll_mmap(int epfd, struct epoll_uheader **_header,
			struct epoll_uindex **_index)
{
	struct epoll_uheader *header;
	struct epoll_uindex *index;
	unsigned int len;

	len = sysconf(_SC_PAGESIZE);
again:
	header = (decltype(header))mmap(NULL, len, PROT_WRITE|PROT_READ, MAP_SHARED, epfd, 0);
	if (header == MAP_FAILED) {
		printf("mmap(header)");
		assert(0);
	}

	if (header->header_length != len) {
		unsigned int tmp_len = len;

		len = header->header_length;
		munmap(header, tmp_len);
		goto again;
	}

	assert(header->magic == EPOLL_USERPOLL_HEADER_MAGIC);

	index = (decltype(index))mmap(NULL, header->index_length, PROT_WRITE|PROT_READ, MAP_SHARED,
				      epfd, header->header_length);
	if (index == MAP_FAILED) {
		printf("mmap(index)");
		assert(0);
	}

	BUILD_BUG_ON(sizeof(*header) != EPOLL_USERPOLL_HEADER_SIZE);
	BUILD_BUG_ON(sizeof(header->items[0]) != 16);

	*_header = header;
	*_index = index;
}

static inline unsigned long long nsecs(void)
{
       struct timespec ts = {0, 0};

       clock_gettime(CLOCK_MONOTONIC, &ts);
       return ((unsigned long long)ts.tv_sec * 1000000000ull) + ts.tv_nsec;
}


int EpollDriver::init(EventCenter *c, int nevent)
{
  events_ts.reserve(100000000);
  /* Start collecting events in 2 secs */
  start_ns = nsecs() + 2ull * 1000000000ull;

  events = (struct epoll_event*)malloc(sizeof(struct epoll_event)*nevent);
  if (!events) {
    lderr(cct) << __func__ << " unable to malloc memory. " << dendl;
    return -ENOMEM;
  }
  memset(events, 0, sizeof(struct epoll_event)*nevent);

#ifdef USE_USERPOLL
  epfd = epoll_create2(EPOLL_USERPOLL, 1024);
  if (epfd >= 0) {
    /* Mmap all pointers */
    uepoll_mmap(epfd, &uheader, &uindex);
    /* XXX */
    printf("!!!!! USE_USEPOLL, spins=%d\n", SPINS);
  }

#else
  epfd = epoll_create(1024); /* 1024 is just an hint for the kernel */
#endif
  if (epfd == -1) {
    lderr(cct) << __func__ << " unable to do epoll_create: "
                       << cpp_strerror(errno) << dendl;
    return -errno;
  }
  if (::fcntl(epfd, F_SETFD, FD_CLOEXEC) == -1) {
    int e = errno;
    ::close(epfd);
    lderr(cct) << __func__ << " unable to set cloexec: "
                       << cpp_strerror(e) << dendl;

    return -e;
  }

  size = nevent;

  return 0;
}

int EpollDriver::add_event(int fd, int cur_mask, int add_mask)
{
  ldout(cct, 20) << __func__ << " add event fd=" << fd << " cur_mask=" << cur_mask
                 << " add_mask=" << add_mask << " to " << epfd << dendl;
  struct epoll_event ee;
  /* If the fd was already monitored for some event, we need a MOD
   * operation. Otherwise we need an ADD operation. */
  int op;
  op = cur_mask == EVENT_NONE ? EPOLL_CTL_ADD: EPOLL_CTL_MOD;

  ee.events = EPOLLET;
  add_mask |= cur_mask; /* Merge old events */
  if (add_mask & EVENT_READABLE)
    ee.events |= EPOLLIN;
  if (add_mask & EVENT_WRITABLE)
    ee.events |= EPOLLOUT;
  ee.data.u64 = 0; /* avoid valgrind warning */
  ee.data.fd = fd;
  if (epoll_ctl(epfd, op, fd, &ee) == -1) {
    lderr(cct) << __func__ << " epoll_ctl: add fd=" << fd << " failed. "
               << cpp_strerror(errno) << dendl;
    return -errno;
  }

  return 0;
}

int EpollDriver::del_event(int fd, int cur_mask, int delmask)
{
  ldout(cct, 20) << __func__ << " del event fd=" << fd << " cur_mask=" << cur_mask
                 << " delmask=" << delmask << " to " << epfd << dendl;
  struct epoll_event ee;
  int mask = cur_mask & (~delmask);
  int r = 0;

  //XXX
  ee.events = EPOLLET;
  if (mask & EVENT_READABLE) ee.events |= EPOLLIN;
  if (mask & EVENT_WRITABLE) ee.events |= EPOLLOUT;
  ee.data.u64 = 0; /* avoid valgrind warning */
  ee.data.fd = fd;
  if (mask != EVENT_NONE) {
    if ((r = epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ee)) < 0) {
      lderr(cct) << __func__ << " epoll_ctl: modify fd=" << fd << " mask=" << mask
                 << " failed." << cpp_strerror(errno) << dendl;
      return -errno;
    }
  } else {
    /* Note, Kernel < 2.6.9 requires a non null event pointer even for
     * EPOLL_CTL_DEL. */
    if ((r = epoll_ctl(epfd, EPOLL_CTL_DEL, fd, &ee)) < 0) {
      lderr(cct) << __func__ << " epoll_ctl: delete fd=" << fd
                 << " failed." << cpp_strerror(errno) << dendl;
      return -errno;
    }
  }
  return 0;
}

int EpollDriver::resize_events(int newsize)
{
  return 0;
}

static inline unsigned int max_index_nr(struct epoll_uheader *header)
{
	return header->index_length >> 4;
}


static unsigned long long s_wait_ns;
static unsigned long long s_spins_nr;
static unsigned long long s_real_waits_nr;
static unsigned long long s_nr;

bool EpollDriver::read_event(struct epoll_uheader *header,
			     struct epoll_uindex *index,
			     unsigned int idx, struct epoll_event *event,
			     uint64_t now)
{
	struct epoll_uitem *item;
	struct epoll_uindex *item_idx_ptr;
	unsigned int indeces_mask;

	indeces_mask = max_index_nr(header) - 1;
	if (indeces_mask & max_index_nr(header)) {
		assert(0);
		/* Should be pow2, corrupted header? */
		return false;
	}

	item_idx_ptr = &index[idx & indeces_mask];

	/*
	 * Spin here till we see valid index
	 */
	while (!(idx = __atomic_load_n(&item_idx_ptr->index, __ATOMIC_ACQUIRE)))
		;

	if (idx > header->max_items_nr) {
		assert(0);
		/* Corrupted index? */
		return false;
	}

	item = &header->items[idx - 1];

	/*
	 * Mark index as invalid, that is for userspace only, kernel does not care
	 * and will refill this pointer only when observes that event is cleared,
	 * which happens below.
	 */
	item_idx_ptr->index = 0;

	append_event_ts(now, item_idx_ptr->ns, 999999999);

	/*
	 * Fetch data first, if event is cleared by the kernel we drop the data
	 * returning false.
	 */
	event->data.u64 = item->data;
	event->events = __atomic_exchange_n(&item->ready_events, 0,
					    __ATOMIC_RELEASE);

	return (event->events & ~EPOLLREMOVED);
}

void EpollDriver::append_event_ts(uint64_t now,
				  uint64_t ns, uint32_t spins)
{
  if (!start_ns || now < start_ns)
    return;

  if (!end_ns) {
    end_ns = now + 5ull * 1000000000ull;
    printf("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ START COLLECTING EVENTS\n");
  }
  else if (now >= end_ns) {
    char filename[] = "/tmp/events-dump.XXXXXX";
    int fd = mkstemp(filename);
    ceph_assert(fd >= 0);

    int64_t comp = 0;
    for (struct ev &ev : events_ts) {
      char buf[128];
      int sz, rc;

      if (!comp)
	comp = ev.ns;

      sz = snprintf(buf, sizeof(buf), "%ld\t%lu\n", ev.ns - comp, ev.spins);
      rc = write(fd, buf, sz);
      ceph_assert(rc == sz);
    }
    close(fd);

    printf("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ STOP COLLECTING EVENTS\n");
    printf("COLLECTED %ld\n", events_ts.size());
    start_ns = 0;
  }

  events_ts.emplace_back(ns, spins);
}

int EpollDriver::uepoll_wait(int ep, struct epoll_event *events,
			     int maxevents, int timeout)

{
	/*
	 * Before entering kernel we do busy wait for ~1ms, naively assuming
	 * each iteration costs 1 cycle, 1 ns.
	 */
	unsigned int spins = SPINS;
	unsigned int tail;
	int i;

	unsigned long long ns, now;

	assert(maxevents > 0);

	ns = nsecs();

again:
	/*
	 * Cache the tail because we don't want refetch it on each iteration
	 * and then catch live events updates, i.e. we don't want user @events
	 * array consist of events from the same fds.
	 */
	tail = READ_ONCE(uheader->tail);

	if (uheader->head == tail && timeout != 0) {
	        if (spins == SPINS)
		  append_event_ts(ns, ns, 0);

	        if (spins--)
			/* Busy loop a bit */
			goto again;

		s_real_waits_nr++;
		i = epoll_wait(ep, NULL, 0, timeout);
		assert(i <= 0);
		if (i == 0 || (i < 0 && errno != ESTALE))
			goto out;

		tail = READ_ONCE(uheader->tail);
		assert(uheader->head != tail);
	}
	for (i = 0; uheader->head != tail && i < maxevents; uheader->head++) {
	       if (read_event(uheader, uindex, uheader->head, &events[i], ns))
			i++;
		else {
			/*
			 * Event cleared by kernel because EPOLL_CTL_DEL was called,
			 * nothing interesting, continue.
			 */
		}
	}
out:
	now = nsecs();
	append_event_ts(ns, now, SPINS - spins + 1);

	s_wait_ns  += now - ns;
	s_spins_nr += SPINS - spins;
	s_nr += 1;
#define WAITS 1000

	if (s_nr == WAITS) {

	  double wait_ns = (double)s_wait_ns / WAITS;

	  double spin_nr = (double)s_spins_nr / WAITS;
	  spin_nr = spin_nr * 100.0 / SPINS;

	  double real_nr = (double)s_real_waits_nr * 100.0 / WAITS;


	  printf(">>>>>>>>>>> wait avg %.3f ns, spins avg %.1f%%, real waits avg %.1f%%\n",
		 wait_ns, spin_nr, real_nr);

//             volatile int spins = SPINS * real_nr * 2;
//             while (spins--)
//                     ;

	  s_wait_ns  = 0;
	  s_spins_nr = 0;
	  s_real_waits_nr = 0;
	  s_nr = 0;
	}

	return i;
}

int EpollDriver::event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
  int retval, numevents = 0;

#ifdef USE_USERPOLL
  retval = uepoll_wait(epfd, events, size,
		       tvp ? (tvp->tv_sec*1000 + tvp->tv_usec/1000) : -1);
#else
  retval = epoll_wait(epfd, events, size,
                      tvp ? (tvp->tv_sec*1000 + tvp->tv_usec/1000) : -1);
#endif
  if (retval > 0) {
    int j;

    numevents = retval;
    fired_events.resize(numevents);
    for (j = 0; j < numevents; j++) {
      int mask = 0;
      struct epoll_event *e = events + j;

      if (e->events & EPOLLIN) mask |= EVENT_READABLE;
      if (e->events & EPOLLOUT) mask |= EVENT_WRITABLE;
      if (e->events & EPOLLERR) mask |= EVENT_READABLE|EVENT_WRITABLE;
      if (e->events & EPOLLHUP) mask |= EVENT_READABLE|EVENT_WRITABLE;
      fired_events[j].fd = e->data.fd;
      fired_events[j].mask = mask;
    }
  }
  return numevents;
}
