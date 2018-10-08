// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include "XioPool.h"

XioPoolStats xp_stats;

bool XioPool::trace_mempool = 0;
bool XioPool::trace_msgcnt = 0;

void XioPoolStats::dump(const char* tag, uint64_t serial)
{
  std::cout
    << tag << " #" << serial << ": "
    << "pool objs: "
    << "64: " << ctr_set[SLAB_64].load() << " "
    << "256: " << ctr_set[SLAB_256].load() << " "
    << "1024: " << ctr_set[SLAB_1024].load() << " "
    << "page: " << ctr_set[SLAB_PAGE].load() << " "
    << "max: " << ctr_set[SLAB_MAX].load() << " "
    << "overflow: " << ctr_set[SLAB_OVERFLOW].load() << " "
    << std::endl;
  std::cout
    << tag << " #" << serial << ": "
    << " msg objs: "
    << "in: " << hook_cnt.load() << " "
    << "out: " << msg_cnt.load() << " "
    << std::endl;
}
