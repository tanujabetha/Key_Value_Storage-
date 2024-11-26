/**
 * Tony Givargis
 * Copyright (C), 2023
 * University of California, Irvine
 *
 * CS 238P - Operating Systems
 * logfs.c
 */

#include <pthread.h>
#include "device.h"
#include "logfs.h"

#define WCACHE_BLOCKS 32
#define RCACHE_BLOCKS 256

/**
 * Needs:
 *   pthread_create()
 *   pthread_join()
 *   pthread_mutex_init()
 *   pthread_mutex_destroy()
 *   pthread_mutex_lock()
 *   pthread_mutex_unlock()
 *   pthread_cond_init()
 *   pthread_cond_destroy()
 *   pthread_cond_wait()
 *   pthread_cond_signal()
 */

/* research the above Needed API and design accordingly */

struct block {
        uint64_t block;
        void *buf;
};

struct logfs {
        struct device* device;
        uint8_t completed;
        pthread_t worker_thread;
        uint64_t off, block_size, available;
        void *cur_block, *cur_off;
        struct wcache_info {
            pthread_mutex_t mutex;
                pthread_cond_t user_cond;
                pthread_cond_t worker_cond;
                uint8_t head;
                uint8_t tail;
                uint8_t written_blocks;
        } wc_info;
        struct block write_cache[WCACHE_BLOCKS], read_cache[RCACHE_BLOCKS];
};

uint64_t get_lower_boundary(struct logfs *logfs, uint64_t off) {
        return (off - (off % logfs->block_size));
}

void flushData(struct logfs* logfs) {
        void *tmp_buf = malloc(2*logfs->block_size);
        void *buf = memory_align(tmp_buf, logfs->block_size);
        struct block *write_cache_block;

        if (0 != pthread_mutex_lock(&logfs->wc_info.mutex)) {
                TRACE("error in flush data");
                exit(1);
        }
        while ((!logfs->completed) &&
               (logfs->wc_info.written_blocks <= 0)) {
                if (0 != pthread_cond_wait(&logfs->wc_info.worker_cond,
                                           &logfs->wc_info.mutex)) {
                        TRACE("error in flush data");
                        exit(1);
                }
        }

        if (logfs->completed) {
                free(tmp_buf);
                if (0 != pthread_mutex_unlock(&logfs->wc_info.mutex)) {
                        TRACE("error in flush data");
                        exit(1);
                }
                return;
        }

        write_cache_block = &logfs->write_cache[logfs->wc_info.tail];
        memcpy(buf, write_cache_block->buf, logfs->block_size);

        logfs->wc_info.tail = (logfs->wc_info.tail + 1) % WCACHE_BLOCKS;
        logfs->wc_info.written_blocks--;
        pthread_cond_signal(&logfs->wc_info.user_cond);

        if (0 != pthread_mutex_unlock(&logfs->wc_info.mutex)) {
                TRACE("error in flush data");
                exit(1);
        }

        if (device_write(logfs->device,
                         buf,
                         write_cache_block->block,
                         logfs->block_size)) {
                TRACE("error in flush data");
                exit(1);
        }
        free(tmp_buf);
}

void* worker_thread(void* arg) {
        struct logfs *logfs = (struct logfs*)arg;
        while (!logfs->completed) {
                flushData(logfs);
        }
        pthread_exit(NULL);
}

struct logfs *logfs_open(const char *pathname) {
        struct logfs* logfs;
        int i;

        logfs = (struct logfs*) malloc(sizeof(struct logfs));

        logfs->device = device_open(pathname);
        if (NULL == logfs->device) {
                TRACE("unable to open device");
                free(logfs);
                return NULL;
        }

        logfs->off = 0;
        logfs->completed = 0;
        logfs->block_size = device_block(logfs->device);
        logfs->wc_info.head = 0;
        logfs->wc_info.tail = 0;
        logfs->wc_info.written_blocks = 0;

        logfs->cur_block = malloc(logfs->block_size);
        memset(logfs->cur_block, 0, logfs->block_size);
        logfs->available = logfs->block_size;

        logfs->cur_off = logfs->cur_block;

        for (i=0; i<RCACHE_BLOCKS; i++) {
                logfs->read_cache[i].block = 7; 
                logfs->read_cache[i].buf = malloc(logfs->block_size);
        }

        for (i=0; i<WCACHE_BLOCKS; i++) {
                logfs->write_cache[i].block = 7; 
                logfs->write_cache[i].buf = malloc(logfs->block_size);
        }

        if (0 != pthread_cond_init(&logfs->wc_info.worker_cond, NULL)) {
                exit(1);
        }

        if (0 != pthread_cond_init(&logfs->wc_info.user_cond, NULL)) {
                exit(1);
        }

        if (0 != pthread_mutex_init(&logfs->wc_info.mutex, NULL)) {
                exit(1);
        }

        if (0 != pthread_create(&logfs->worker_thread, NULL, &worker_thread, (void*)logfs)) {
                exit(1);
        }

        return logfs;
}

void logfs_close(struct logfs* logfs) {
        int i;       
        logfs->completed = 1;
        pthread_cond_signal(&logfs->wc_info.worker_cond);
        pthread_cond_signal(&logfs->wc_info.user_cond);
        pthread_mutex_destroy(&logfs->wc_info.mutex);
        pthread_cond_destroy(&logfs->wc_info.user_cond);
        pthread_cond_destroy(&logfs->wc_info.worker_cond);

        if (0 != pthread_join(logfs->worker_thread, NULL)) {
                TRACE("joining thread fail");
        }
        device_close(logfs->device);
        free(logfs->cur_block);
        for (i=0; i<RCACHE_BLOCKS; i++) {
                free(logfs->read_cache[i].buf);
        }
        for (i=0; i<WCACHE_BLOCKS; i++) {
                free(logfs->write_cache[i].buf);
        }
        memset(logfs, 0, sizeof(struct logfs));
        free(logfs);
}

int logfs_append(struct logfs *logfs, const void *buf, uint64_t len) {
        void *buf_;
        int readCacheOffset;
        uint64_t block, remaining;

        buf_ = (void*)buf;
        remaining = len;
        block = get_lower_boundary(logfs, logfs->off);
        readCacheOffset = (block / logfs->block_size) % RCACHE_BLOCKS;
        if (block == logfs->read_cache[readCacheOffset].block) {
                logfs->read_cache[readCacheOffset].block = 7;
        }
        while(remaining >= logfs->available) {
                memcpy(logfs->cur_off, buf_, logfs->available);
                buf_ = (void*)((char*)buf_ + logfs->available);
                logfs->off += logfs->available;
                remaining -= logfs->available;
                logfs->available = 0;
        struct block *write_cache_block;
        if (0 != pthread_mutex_lock(&logfs->wc_info.mutex)) {
                TRACE("error in write_to_cache");
                exit(1);
        }
        while (logfs->wc_info.written_blocks >= WCACHE_BLOCKS) {
                if (0 != pthread_cond_wait(&logfs->wc_info.user_cond,
                                           &logfs->wc_info.mutex)) {
                        TRACE("error in write_to_cache");
                        exit(1);
                }
        }

        write_cache_block = &logfs->write_cache[logfs->wc_info.head];
        write_cache_block->block = get_lower_boundary(logfs, logfs->off - 1);
        memcpy(write_cache_block->buf, logfs->cur_block, logfs->block_size);

        logfs->wc_info.head = (logfs->wc_info.head + 1) % WCACHE_BLOCKS;
        logfs->wc_info.written_blocks++;
        pthread_cond_signal(&logfs->wc_info.worker_cond);

        if (0 != pthread_mutex_unlock(&logfs->wc_info.mutex)) {
                TRACE("error in write_to_cache");
                exit(1);
        }
                memset(logfs->cur_block, 0, logfs->block_size);
                logfs->available = logfs->block_size;
                logfs->cur_off = logfs->cur_block;
        }
        if (0 < remaining){
                memcpy(logfs->cur_off, buf_, remaining);
                logfs->off += remaining;
                logfs->cur_off = (void*)((char*)logfs->cur_off + remaining);
                logfs->available -= remaining;
        }
        return 0;
}

int check_in_writeCache(struct logfs *logfs, const uint64_t block_start_address, void *buf) {
        int i, found;
        pthread_mutex_lock(&logfs->wc_info.mutex);
        found = 0;
        for (i=0; i<WCACHE_BLOCKS; i++) {
                if (block_start_address == logfs->write_cache[i].block) {
                        memcpy(buf, logfs->write_cache[i].buf, logfs->block_size);
                        found = 1;
                        break;
                }
        }
        pthread_mutex_unlock(&logfs->wc_info.mutex);
        return found;
}

int read_data(struct logfs *logfs, const uint64_t block_start_address, void *buf, uint64_t off, size_t len) {
        int readCacheOffset;
        uint64_t block_off;
        void *start;
        struct block *read_cache_block;
        void *tmp_buf = malloc(2*logfs->block_size);
        void *buf_ = memory_align(tmp_buf, logfs->block_size);

        readCacheOffset = (block_start_address / logfs->block_size) % RCACHE_BLOCKS;
        read_cache_block = &logfs->read_cache[readCacheOffset];

        if (block_start_address != read_cache_block->block) {
                read_cache_block->block = block_start_address;
                if (block_start_address == get_lower_boundary(logfs, logfs->off)) {
                        memcpy(buf_, logfs->cur_block, logfs->block_size);
                }
                else if (check_in_writeCache(logfs, block_start_address, buf_)) {
                        assert(1);
                }
                else {
                        if (device_read(logfs->device, buf_,
                                        block_start_address, logfs->block_size)) {
                                return -1;
                        }
                }
                memcpy(read_cache_block->buf, buf_, logfs->block_size);
        }
        free(tmp_buf);
        block_off = off - block_start_address;
        start = (void*)((char*)read_cache_block->buf + block_off);
        memcpy(buf, start, len);
        return 0;
}

int logfs_read(struct logfs *logfs, void *buf, uint64_t off, size_t len) {
        void *buf_;
        uint64_t off_, read_len, block_start_address, remaining;
        buf_ = buf;
        off_ = off;
        remaining = len;
        block_start_address = get_lower_boundary(logfs, off);
        read_len = MIN(len, block_start_address + logfs->block_size - off);

        if (read_data(logfs, block_start_address, buf_, off_, read_len)) {
                return -1;
        }
        block_start_address += logfs->block_size;
        buf_ = (void*)((char*)buf_ + read_len);
        off_ += read_len;
        remaining -= read_len;

        while(remaining > logfs->block_size) {
                read_len = logfs->block_size;
                if (read_data(logfs, block_start_address, buf_, off_, read_len)) {
                        return -1;
                }
                block_start_address += logfs->block_size;
                buf_ = (void*)((char*)buf_ + read_len);
                off_ += read_len;
                remaining -= read_len;
        }
        if (0 != remaining) {
                if (read_data(logfs, block_start_address, buf_, off_, remaining)) {
                        return -1;
                }
        }
        return 0;
}
