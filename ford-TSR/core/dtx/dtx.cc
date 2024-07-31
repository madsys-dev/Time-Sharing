// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"
#include <cstdlib>

DTX::DTX(MetaManager* meta_man,
         QPManager* qp_man,
         VersionCache* status,
         LockCache* lock_table,
         t_id_t tid,
         coro_id_t coroid,
         CoroutineScheduler* sched,
         RDMABufferAllocator* rdma_buffer_allocator,
         LogOffsetAllocator* remote_log_offset_allocator,
         AddrCache* addr_buf) {
  // Transaction setup
  tx_id = 0;
  t_id = tid;
  coro_id = coroid;
  coro_sched = sched;
  global_meta_man = meta_man;
  thread_qp_man = qp_man;
  global_vcache = status;
  global_lcache = lock_table;
  thread_rdma_buffer_alloc = rdma_buffer_allocator;
  tx_status = TXStatus::TX_INIT;

  select_backup = 0;
  thread_remote_log_offset_alloc = remote_log_offset_allocator;
  addr_cache = addr_buf;

  hit_local_cache_times = 0;
  miss_local_cache_times = 0;
}

bool DTX::ExeRO(coro_yield_t& yield) {
  // You can read from primary or backup
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;

  // Issue reads
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read ro";
  if (!IssueReadRO(pending_direct_ro, pending_hash_ro)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // Receive data
  std::list<InvisibleRead> pending_invisible_ro;
  std::list<HashRead> pending_next_hash_ro;
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " check read ro";
  auto res = CheckReadRO(pending_direct_ro, pending_hash_ro, pending_invisible_ro, pending_next_hash_ro, yield);
  return res;
}

bool DTX::ExeRW(coro_yield_t& yield) {
  // For read-only data from primary or backup
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;

  // For read-write data from primary
  std::vector<CasRead> pending_cas_rw;
  std::vector<DirectRead> pending_direct_rw;
  std::vector<HashRead> pending_hash_rw;
  std::vector<InsertOffRead> pending_insert_off_rw;

  std::list<InvisibleRead> pending_invisible_ro;

  std::list<HashRead> pending_next_hash_ro;
  std::list<HashRead> pending_next_hash_rw;
  std::list<InsertOffRead> pending_next_off_rw;

  if (!IssueReadRO(pending_direct_ro, pending_hash_ro))  {
    // printf("check ro error\n");
  return false;}  // RW transactions may also have RO data
// RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read rorw";
#if READ_LOCK
  if (!IssueReadLock(pending_cas_rw, pending_hash_rw, pending_insert_off_rw)) {
    // printf("check rl error\n");
  return false;}
#else
  if (!IssueReadRW(pending_direct_rw, pending_hash_rw, pending_insert_off_rw)) return false;
#endif

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " check read rorw";
  bool res = false;
#if READ_LOCK
  res = CheckReadRORW(pending_direct_ro,
                      pending_hash_ro,
                      pending_hash_rw,
                      pending_insert_off_rw,
                      pending_cas_rw,
                      pending_invisible_ro,
                      pending_next_hash_ro,
                      pending_next_hash_rw,
                      pending_next_off_rw,
                      yield);
  if(!res) {
    // printf("check error\n");
  }
#else
  res = CompareCheckReadRORW(pending_direct_ro,
                             pending_direct_rw,
                             pending_hash_ro,
                             pending_hash_rw,
                             pending_next_hash_ro,
                             pending_next_hash_rw,
                             pending_insert_off_rw,
                             pending_next_off_rw,
                             pending_invisible_ro,
                             yield);
#endif

#if COMMIT_TOGETHER
  ParallelUndoLog();
#endif

  return res;
}

bool DTX::Validate(coro_yield_t& yield) {
  // RDMA_LOG(INFO) << "validate?";
  // The transaction is read-write, and all the written data have been locked before
  if (not_eager_locked_rw_set.empty() && read_only_set.empty()) {
    // TLOG(DBG, t_id) << "save validation";
    return true;
  }

  std::vector<ValidateRead> pending_validate;

#if LOCAL_VALIDATION
  ValStatus ret = IssueLocalValidate(pending_validate);

  if (ret == ValStatus::NO_NEED_VAL) {
    return true;
  } else if (ret == ValStatus::RDMA_ERROR || ret == ValStatus::MUST_ABORT) {
    return false;
  }
#else
  if (!IssueRemoteValidate(pending_validate)) return false;
#endif

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  auto res = CheckValidate(pending_validate);
  return res;
}

// Invisible + write primary and backups
bool DTX::CoalescentCommit(coro_yield_t& yield) {
  tx_status = TXStatus::TX_COMMIT;
  char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
#if LOCAL_LOCK
  *(lock_t*)cas_buf = 0;
#else
  *(lock_t*)cas_buf = STATE_LOCKED | STATE_INVISIBLE;
#endif

  std::vector<CommitWrite> pending_commit_write;

  // Check whether all the log ACKs have returned
  // TODO 调整log的时机
#if TSR_LOCAL
  // *(lock_t*)cas_buf = STATE_INVISIBLE;
#else
  while (!coro_sched->CheckLogAck(coro_id)) {
    ;  // wait
  }
#endif
#if RFLUSH == 0
  if (!IssueCommitAll(pending_commit_write, cas_buf)) return false;
#elif RFLUSH == 1
  if (!IssueCommitAllFullFlush(pending_commit_write, cas_buf)) return false;
#elif RFLUSH == 2
  if (!IssueCommitAllSelectFlush(pending_commit_write, cas_buf)) return false;
#endif
  
  // 这个会等所有的结果返回
  coro_sched->Yield(yield, coro_id);

  *((lock_t*)cas_buf) = 0;
  
  auto res = CheckCommitAll(pending_commit_write, cas_buf);

  return res;
}

// TSR+
void DTX::ParallelUndoLog() {
  // Write the old data from read write set
#if TSR_LOCAL
  // *(lock_t*)cas_buf = STATE_INVISIBLE;
  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      batch_pending_log_set.push_back(set_it);
      set_it.is_logged = true;
    }
  }
#else
  size_t log_size = sizeof(tx_id) + sizeof(t_id);
  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      // For the newly inserted data, the old data are not needed to be recorded
      log_size += DataItemSize;
    }
  }
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);

  offset_t cur = 0;
  *((tx_id_t*)(written_log_buf + cur)) = tx_id;
  cur += sizeof(tx_id);
  *((t_id_t*)(written_log_buf + cur)) = t_id;
  cur += sizeof(t_id);

  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      memcpy(written_log_buf + cur, (char*)(set_it.item_ptr.get()), DataItemSize);
      cur += DataItemSize;
      set_it.is_logged = true;
    }
  }

  // Write undo logs to all memory nodes
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(i, log_size);
    RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
    coro_sched->RDMALog(coro_id, tx_id, qp, written_log_buf, log_offset, log_size);
  }
#endif
}
void DTX::PollCompletion() {
  coro_sched->PollRegularCompletion();
}
bool DTX::UpdateTSR(char flag) {
  // coro_sched->PollCompletion();

  char* wt_data = thread_rdma_buffer_alloc->Alloc(8);
  wt_data[3] = flag;
  RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(0);
  struct timespec t0, t1, t2;
    clock_gettime(CLOCK_REALTIME, &t0);
  int rand_wr = 123;//rand()%1000;
  auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, 8, 24, IBV_SEND_SIGNALED | IBV_SEND_INLINE, rand_wr);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll write fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  assert(wc.wr_id == rand_wr);
  // rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, 8, 20, IBV_SEND_SIGNALED | IBV_SEND_FENCE, 231);
  // if (rc != SUCC) {
  //   RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
  //   return false;
  // }
  // rc = qp->poll_till_completion(wc, no_timeout);
  // if (rc != SUCC) {
  //   RDMA_LOG(ERROR) << "client: poll write fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
  //   return false;
  // }
  // assert(wc.wr_id == 231);
  clock_gettime(CLOCK_REALTIME, &t1);

  // char x = GetTSR();
  // // fprintf(stderr, "read_flag first: %d\n", x);
  // while(x != flag) {
  //   // usleep(1);
  //   x = GetTSR();
  // }
  // clock_gettime(CLOCK_REALTIME, &t2);

  // RDMA_LOG(INFO)<< "update finish, read " << (int) x << ", time duringb " << t2.tv_nsec - t1.tv_nsec << " " << t1.tv_nsec-t0.tv_nsec;

  // char f = GetTSR();
  // RDMA_LOG(INFO) << "read_flag-0 " <<  (int)f;
  return true;
}

char DTX::GetTSR() {
//     struct timespec t0, t1;
// clock_gettime(CLOCK_REALTIME, &t0);
  char* rd_data = thread_rdma_buffer_alloc->Alloc(8);
  RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(0);
  int rand_wr = 145;//rand()%1000;
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, 8, 24, IBV_SEND_SIGNALED, rand_wr);
// clock_gettime(CLOCK_REALTIME, &t1);
// if(t1.tv_nsec < t0.tv_nsec)
  // { printf("read \n"); }
  usleep(1);
  // asm volatile("mfence");

  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return 2;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return 2;
  }
  assert(wc.wr_id == rand_wr);
  return rd_data[3];
}

void DTX::Abort() {
  // When failures occur, transactions need to be aborted.
  // In general, the transaction will not abort during committing replicas if no hardware failure occurs
  char* unlock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  *((lock_t*)unlock_buf) = 0;
  for (auto& index : locked_rw_set) {
    auto& it = read_write_set[index].item_ptr;
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* primary_qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    auto rc = primary_qp->post_send(IBV_WR_RDMA_WRITE, unlock_buf, sizeof(lock_t), it->GetRemoteLockAddr(), 0);
    if (rc != SUCC) {
      RDMA_LOG(FATAL) << "Thread " << t_id << " , Coroutine " << coro_id << " unlock fails during abortion";
    }
  }
  tx_status = TXStatus::TX_ABORT;
  LocalUnlock();
}

void DTX::BatchWrite() {
  for (auto& set_it : batch_pending_read_write_set) {
    char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    auto it = set_it.item_ptr;
    // Maintain the version that user specified
    if (!it->user_insert) {
      it->version = tx_id;
    }
    it->lock = 0;
    memcpy(data_buf, (char*)it.get(), DataItemSize);

    // Commit primary
    node_id_t node_id = global_meta_man->GetPrimaryNodeID(it->table_id);  // Read-write data can only be read from primary
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(node_id);
    if (!coro_sched->RDMAWrite(coro_id, qp, data_buf, it->remote_offset, DataItemSize)) {
      return ;
    }
  }
}

void DTX::CleanBatch() {

  size_t log_size = sizeof(tx_id) + sizeof(t_id);
  if(batch_pending_log_set.size() == 0) {
    return ;
  }
// printf("log size: %d %d\n", batch_pending_log_set.size(), batch_pending_read_write_set.size());
  // printf("clean %d\n", batch_pending_log_set.size());
  for (auto& set_it : batch_pending_log_set) {
    if (!set_it.item_ptr->user_insert) {
      // For the newly inserted data, the old data are not needed to be recorded
      log_size += DataItemSize;
    }
  }
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);

  offset_t cur = 0;
  *((tx_id_t*)(written_log_buf + cur)) = tx_id;
  cur += sizeof(tx_id);
  *((t_id_t*)(written_log_buf + cur)) = t_id;
  cur += sizeof(t_id);

  for (auto& set_it : batch_pending_log_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      memcpy(written_log_buf + cur, (char*)(set_it.item_ptr.get()), DataItemSize);
      cur += DataItemSize;
      set_it.is_logged = true;
    }
  }

  // Write undo logs to all memory nodes
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(i, log_size);
    RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
    coro_sched->RDMALog(coro_id, tx_id, qp, written_log_buf, log_offset, log_size);
  }

  // if (batch_pending_read_write_set.size() == 0) 
  //   return ;
  char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

  int current_i = 0;
  for (auto& set_it : batch_pending_read_write_set) {
    // char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);

    auto it = set_it.item_ptr;
    // Commit backup
    // Get the offset (item's addr relative to table's addr) in backup
    // The offset is the same with that in primary
    const HashMeta& primary_hash_meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
    auto offset_in_backup_hash_store = it->remote_offset - primary_hash_meta.base_off;

    // Get all the backup queue pairs and hash metas for this table
    auto* backup_node_ids = global_meta_man->GetBackupNodeID(it->table_id);
    if (!backup_node_ids) continue;  // There are no backups in the PM pool
    const std::vector<HashMeta>* backup_hash_metas = global_meta_man->GetBackupHashMetasWithTableID(it->table_id);
    // backup_node_ids guarantees that the order of remote machine is the same in backup_hash_metas and backup_qps
    
    // TODO Backup_update 目前是每项分别的，要改成一次性primary - backup

      for (size_t i = 0; i < backup_node_ids->size(); i++) {
        auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
        auto remote_lock_off = it->GetRemoteLockAddr(remote_item_off);

        char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
        it->lock = STATE_INVISIBLE;
        it->remote_offset = remote_item_off;
        memcpy(data_buf, (char*)it.get(), DataItemSize);
        RCQP* backup_qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));

        // if (!coro_sched->RDMAWrite(coro_id, backup_qp, data_buf, remote_item_off, DataItemSize)) {
        //   return false;
        // }
        std::shared_ptr<InvisibleWriteBatch> doorbell = std::make_shared<InvisibleWriteBatch>();
        doorbell->SetWriteRemoteReq(data_buf, remote_item_off, DataItemSize);
        if (!doorbell->SendReqs2(coro_sched, backup_qp, coro_id, 0)) {
          break;
        }
        // Selective Remote FLUSH: Only flush the last data that is written to backup
        if (current_i == batch_pending_read_write_set.size() - 1) {
          char* flush_buf = thread_rdma_buffer_alloc->Alloc(RFlushReadSize);
          if (!coro_sched->RDMARead(coro_id, backup_qp, flush_buf, it->remote_offset, RFlushReadSize)) {
            break;
          }
        }
      }
    current_i++;
  }

  coro_sched->PollLogCompletion(); 
  // RDMA_LOG(INFO) << "clean, pending log size:" <<  coro_sched->get_pending_log_size();

  batch_pending_log_set.clear();

  batch_pending_read_write_set.clear();
}
