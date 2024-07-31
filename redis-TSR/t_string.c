
void msetGenericCommand(client *c, int nx) {
    int j;
    // printf("%d\n", nx);

    if ((c->argc % 2) == 0) {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }

    /* Handle the NX flag. The MSETNX semantic is to return zero and don't
     * set anything if at least one key already exists. */
    if (nx) {
        for (j = 1; j < c->argc; j += 2) {
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {
                addReply(c, shared.czero);
                return;
            }
        }
    }

    for (j = 1; j < c->argc; j += 2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
    }
    // printf("working\n");
    // int execute = 1;
    // while(execute > 0) {
    // execute = 0;
    struct timespec begin, end;

REEXECUTE: ;

    int mem_error = 0;
    for (int current_db = list_begin(&server.r_table[server.r_head]->memorys); current_db != -1; current_db = list_next(&server.r_table[server.r_head]->memorys, current_db, TRUE))
    {
        int count = 0;

        // printf("current_db %d\n", current_db);
        clock_gettime(CLOCK_REALTIME, &begin);

        while (1) {
            count ++;
            if (current_db == server.r_head) {
                // handle add compute node
                if (server.r_table[server.r_head]->error_no == R_PREPARE) {
                    sleep_for_nanos(WATI_TIME);
                    continue;
                }
                if (server.r_table[server.r_head]->error_no == R_ADD_NEW_COMPUTE_NODE) {
                    server.r_table[server.r_head]->slot[server.r_cur_id] = 1;
                    // printf("add new compute node\n");
                    server.r_total_computes ++;
                    while(server.r_table[server.r_head]->error_no == R_ADD_NEW_COMPUTE_NODE) {sleep_for_nanos(WATI_TIME);}
                    r_sync_list(0);
                    // r_print_list(0);
                    // printf("update slot %d %x, prev = %d, next = %d\n", server.r_cur_id, &server.r_table[server.r_head]->slot[server.r_cur_id], server.prev_id, server.next_id);
                    // execute = 1;


                    clock_gettime(CLOCK_REALTIME, &end);
                    long seconds = end.tv_sec - begin.tv_sec;
                    long nanoseconds = end.tv_nsec - begin.tv_nsec;
                    double elapsed = seconds * 1e9 + nanoseconds;
                    printf("handle_add compute, time = %lfns\n", elapsed);
                    count = 0;
                    clock_gettime(CLOCK_REALTIME, &begin);

                }
                // handle add memory node
                
                if (server.r_table[server.r_head]->error_no == R_ADD_MEMORY) {
                    // clock_gettime(CLOCK_REALTIME, &begin);
                    // printf("receive \"add memory\" head = %d\n", server.r_head);
                    server.r_table[server.r_head]->slot[server.r_cur_id] = 1;
                    // printf("update slot %d %x\n", server.r_cur_id, &server.r_table[server.r_head]->slot[server.r_cur_id]);
                    // server.r_table[server.r_total_memorys++] = server.r_table[server.r_head]->new_memory;
                    server.mempool_total_server++;

                    while(server.r_table[server.r_head]->error_no ==R_ADD_MEMORY) {sleep_for_nanos(WATI_TIME);}
                    // printf("finish \"add memory\" head = %d\n", server.r_head);


                    clock_gettime(CLOCK_REALTIME, &end);
                    long seconds = end.tv_sec - begin.tv_sec;
                    long nanoseconds = end.tv_nsec - begin.tv_nsec;
                    double elapsed = seconds * 1e9 + nanoseconds;
                    printf("handle_add memory, time = %lfns, count = %d\n", elapsed, count);
                    count = 0;
                    clock_gettime(CLOCK_REALTIME, &begin);

                    // execute = 1;
                }
            }
            
            // handle COMPUTE_ERROR signal
            if (server.r_table[current_db]->error_no == R_COMPUTE_ERROR) {
                // clock_gettime(CLOCK_REALTIME, &begin);

                server.r_table[current_db]->slot[server.r_cur_id] = 1;
                while(server.r_table[current_db]->error_no == R_COMPUTE_ERROR) {sleep_for_nanos(WATI_TIME);}
                server.r_total_computes --;
                r_sync_list(current_db);
                // printf("node %d, compute error end (%d, %d)\n", current_db, server.prev_id, server.next_id);
                clock_gettime(CLOCK_REALTIME, &end);
                long seconds = end.tv_sec - begin.tv_sec;
                long nanoseconds = end.tv_nsec - begin.tv_nsec;
                double elapsed = seconds * 1e9 + nanoseconds;
                printf("find_compute_error, time = %lfns\n", elapsed);
                count = 0;
                clock_gettime(CLOCK_REALTIME, &begin);
                goto REEXECUTE;
            }
            // handle Memory_ERROR signal
            if (mem_error) {
                printf("node %d, wait memory error\n", current_db);

                while(server.r_table[current_db]->error_no != R_MEMORY_ERROR) {sleep_for_nanos(WATI_TIME);}

                // printf("node %d, find memory error %d\n", current_db, server.r_table[current_db]->error_no);
                server.r_table[current_db]->slot[server.r_cur_id] = 1;
                while(server.r_table[current_db]->error_no == R_MEMORY_ERROR) {server.r_table[current_db]->slot[server.r_cur_id] = 1; sleep_for_nanos(WATI_TIME);}
                // total_memorys --;
                server.r_head = get_table_head(server.r_table[current_db]);
                // printf("node %d, update head %d\n", server.r_table[current_db], server.r_head);

                mem_error = 0;

                clock_gettime(CLOCK_REALTIME, &end);
                long seconds = end.tv_sec - begin.tv_sec;
                long nanoseconds = end.tv_nsec - begin.tv_nsec;
                double elapsed = seconds * 1e9 + nanoseconds;
                printf("find_memory_error, time = %lfns\n", elapsed);
                count = 0;  
                clock_gettime(CLOCK_REALTIME, &begin);
                
            }
            int ret = mempoolLoadDb(&server.db[current_db], current_db, server.mempool_role);
            // A memory node exception is detected, but is not part of its own time slice, so it skips that memory and participates in the poll at the next memory
            if (MEMPOOL_RECOVERY_BADMAGIC == ret && (server.r_table[current_db]->error_no == R_DESTROY_MEMORY)) {
                printf("find_memory_error\n");

                clock_gettime(CLOCK_REALTIME, &begin);
                // printf("find distroy error %d by another node\n", server.r_table[current_db]->id, server.r_table[current_db]->error_no);
                mem_error = 1;
                count = 0;
                clock_gettime(CLOCK_REALTIME, &begin);
                break;
            }
            if (MEMPOOL_RECOVERY_BADMAGIC != ret) {
                //  My TimeSlice
                if (server.r_table[current_db]->error_no == R_DESTROY_MEMORY) {
                    // Memory node exception detected and signalled
                    clock_gettime(CLOCK_REALTIME, &begin);
                    printf("handle_memory_error\n");

                    // printf("node %d, memory error\n", current_db);
                    int k = list_next(&server.r_table[current_db]->memorys, current_db, 0);
                    r_prepare_slot(k);
                    server.r_table[k]->error_no = R_MEMORY_ERROR;
                    printf("node %d, wait\n", k);

                    server.r_table[k]->slot[server.r_cur_id] = 1;
                    r_wait_slot(k);
                    for (int j = list_begin(&server.r_table[k]->memorys); j!= -1; j = list_next(&server.r_table[k]->memorys, j, 1)) {
                        // printf("remove %d %d\n", j, current_db);
                        if (j != k)
                            list_remove(&server.r_table[j]->memorys, current_db);
                    }
                    
                    list_remove(&server.r_table[k]->memorys, current_db);
                    if (current_db == server.r_head) {
                        server.r_head = get_table_head(server.r_table[k]);
                    }
                    // total_memorys --;
                    clock_gettime(CLOCK_REALTIME, &end);
                    long seconds = end.tv_sec - begin.tv_sec;
                    long nanoseconds = end.tv_nsec - begin.tv_nsec;
                    double elapsed = seconds * 1e9 + nanoseconds;
                    printf("handle_memory_error, time = %lfns\n", elapsed);
                    server.r_table[k]->error_no = R_RUNNING;

                    break;
                }
                if (server.r_table[current_db]->error_no != R_RUNNING) {
                    printf("error %d\n", server.r_table[current_db]->error_no);
                } 
                //execute
                clock_gettime(CLOCK_REALTIME, &begin);

                for (j = 1; j < c->argc; j += 2) {

                    if (current_db != server.r_head && ((const char*)c->argv[j]->ptr)[0] == 'r') {
                        continue;
                    }
                    robj *val = dupObject2MemPool(c->argv[j+1], server.db[current_db].arena);
                    setKey(c,&server.db[current_db],c->argv[j], val);
                    decrRefCountMemPool(val, server.db[current_db].arena);
                }
                // clock_gettime(CLOCK_REALTIME, &end);
                // double seconds, nanoseconds, elapsed;
                // seconds = end.tv_sec - begin.tv_sec;
                // nanoseconds = end.tv_nsec - begin.tv_nsec;
                // elapsed = seconds * 1e9 + nanoseconds;
                // printf("run mset %ld, time = %lfns\n", c->argc/2, elapsed);

                mempoolSaveDb(&server.db[current_db]);
                // clock_gettime(CLOCK_REALTIME, &end);
                // seconds = end.tv_sec - begin.tv_sec;
                // nanoseconds = end.tv_nsec - begin.tv_nsec;
                // elapsed = seconds * 1e9 + nanoseconds;
                // printf("run mset total, time = %lfns\n", elapsed);
                server.r_table[current_db]->id = server.next_id;
                break;
            }
            // sleep_for_nanos(10000);

            // Detect a compute node anomaly and send a signal
            clock_gettime(CLOCK_REALTIME, &end);
            long seconds = end.tv_sec - begin.tv_sec;
            long nanoseconds = end.tv_nsec - begin.tv_nsec;
            double elapsed = seconds * 1e9 + nanoseconds;
            if (server.r_table[server.r_head]->error_no == R_RUNNING && server.r_table[current_db]->id == server.prev_id && elapsed > 1000000) {
                // clock_gettime(CLOCK_REALTIME, &begin);
                // printf("node %d, compute error %d\n", current_db, server.r_table[current_db]->error_no);
                clock_gettime(CLOCK_REALTIME, &end);
                
                long seconds = end.tv_sec - begin.tv_sec;
                long nanoseconds = end.tv_nsec - begin.tv_nsec;
                double elapsed = seconds * 1e9 + nanoseconds;
                printf("find_compute_error, time = %lfns, count = %d, error_no = %d\n", elapsed, count, server.r_table[server.r_head]->error_no);
                clock_gettime(CLOCK_REALTIME, &begin);

                r_prepare_slot(current_db);
                server.r_table[current_db]->error_no = R_COMPUTE_ERROR;
                server.r_table[current_db]->slot[server.r_cur_id] = 1;
                for (int j = list_begin(&server.r_table[server.r_head]->memorys); j != -1; j = list_next(&server.r_table[current_db]->memorys, j, TRUE)) {
                    list_remove(&server.r_table[j]->computes, server.prev_id);
                }
                r_sync_list(current_db);
                r_wait_slot(current_db);
                // Reach consensus and begin recovery
                r_recovery(current_db);
                // printf("recovery finish\n");
                // print_mem();
                server.r_total_computes --;
                server.r_table[current_db]->error_no = R_RUNNING;
                count = 0;

                clock_gettime(CLOCK_REALTIME, &end);
                seconds = end.tv_sec - begin.tv_sec;
                nanoseconds = end.tv_nsec - begin.tv_nsec;
                elapsed = seconds * 1e9 + nanoseconds;
                printf("handle_compute_error, time = %lfns\n", elapsed);
                goto REEXECUTE;
            }
        }
        // clock_gettime(CLOCK_REALTIME, &end);
        // long seconds = end.tv_sec - begin.tv_sec;
        // long nanoseconds = end.tv_nsec - begin.tv_nsec;
        // double elapsed = seconds * 1e9 + nanoseconds;
        // printf("count %d, db.id = %d, prev_id = %d, time = %lfns\n", count, server.r_table[current_db]->id, server.next_id, elapsed);

        // if(execute > 0)
        // {
        //     break;
        // }    
        // for (j = 1; j < c->argc; j += 2) {
        //     robj *val = dupObject2MemPool(c->argv[j+1], server.db[current_db].arena);
        //     setKey(c,&server.db[current_db],c->argv[j], val);
        //     decrRefCountMemPool(val, server.db[current_db].arena);
        // }
        // mempoolSaveDb(&server.db[current_db]);
        

    }
    int current_db = 0;
    if (mem_error) {
        printf("node %d, wait memory error\n", current_db);

        while(server.r_table[current_db]->error_no != R_MEMORY_ERROR) {sleep_for_nanos(WATI_TIME);}

        // printf("node %d, find memory error %d\n", current_db, server.r_table[current_db]->error_no);
        server.r_table[current_db]->slot[server.r_cur_id] = 1;
        while(server.r_table[current_db]->error_no == R_MEMORY_ERROR) {server.r_table[current_db]->slot[server.r_cur_id] = 1; sleep_for_nanos(WATI_TIME);}
        // total_memorys --;
        server.r_head = get_table_head(server.r_table[current_db]);
        // printf("node %d, update head %d\n", server.r_table[current_db], server.r_head);

        mem_error = 0;

        clock_gettime(CLOCK_REALTIME, &end);
        long seconds = end.tv_sec - begin.tv_sec;
        long nanoseconds = end.tv_nsec - begin.tv_nsec;
        double elapsed = seconds * 1e9 + nanoseconds;
        printf("find_memory_error, time = %lfns\n", elapsed);
        // count = 0;  
        clock_gettime(CLOCK_REALTIME, &begin);
    }
    server.dirty += (c->argc-1)/2;
    addReply(c, nx ? shared.cone : shared.ok);
}