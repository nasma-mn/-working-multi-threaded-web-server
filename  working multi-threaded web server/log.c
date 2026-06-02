#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include "log.h"

#define INIT_SIZE 1024

struct Server_Log {
    char *m_buffer;
    int m_used;
    int m_can_use;
    pthread_mutex_t m_lock;
    pthread_cond_t m_read_allowed_cv;
    pthread_cond_t m_write_allowed_cv;
    int m_readers_inside, m_writers_inside, m_writers_waiting;
    int debug_sleep_seconds;

};
void set_log_debug_sleep(server_log log, int sleep_seconds)
{
    if (!log) return;
    pthread_mutex_lock(&(log->m_lock));
    log->debug_sleep_seconds = sleep_seconds;
    pthread_mutex_unlock(&(log->m_lock));
}

// reader-writer locks as seen in tut.8

static void writer_lock(server_log log) {
    pthread_mutex_lock(&(log->m_lock));
    log->m_writers_waiting++;
    while (log->m_readers_inside + log->m_writers_inside >0){
        pthread_cond_wait(&(log->m_write_allowed_cv), &(log->m_lock));
    }
    log->m_writers_waiting--;
    log->m_writers_inside = 1;
    pthread_mutex_unlock(&(log->m_lock));
}

static void writer_unlock(server_log log) {
    pthread_mutex_lock(&(log->m_lock));
    log->m_writers_inside = 0;

    if (log->m_writers_waiting > 0){ //now a writer can enter
        pthread_cond_signal(&(log->m_write_allowed_cv));
    } else { //or all of the readers
        pthread_cond_broadcast(&(log->m_read_allowed_cv));
    }

    pthread_mutex_unlock(&(log->m_lock));

}

static void reader_lock(server_log log) {
    pthread_mutex_lock(&(log->m_lock));
    while (log->m_writers_inside > 0 || log->m_writers_waiting > 0){
        pthread_cond_wait(&(log->m_read_allowed_cv), &(log->m_lock));
    }
    log->m_readers_inside++;
    pthread_mutex_unlock(&(log->m_lock));
}

static void reader_unlock(server_log log) {
    pthread_mutex_lock(&(log->m_lock));
    log->m_readers_inside--;
// && log->m_writers_waiting > 0
    if(log->m_readers_inside == 0){
        pthread_cond_signal(&(log->m_write_allowed_cv));
    }
    pthread_mutex_unlock(&(log->m_lock));
}

server_log create_log() {
    server_log log = malloc(sizeof(struct Server_Log));
    log->m_buffer = malloc(INIT_SIZE);
    log->m_used = 0;
    log->m_can_use = INIT_SIZE;

    pthread_mutex_init(&(log->m_lock), NULL);
    pthread_cond_init(&(log->m_read_allowed_cv), NULL);
    pthread_cond_init(&(log->m_write_allowed_cv), NULL);

    log->m_readers_inside = 0;
    log->m_writers_inside = 0;
    log->m_writers_waiting = 0;
    log->debug_sleep_seconds = 0;


    return log;
}

void destroy_log(server_log log) {
    free(log->m_buffer);
    pthread_mutex_destroy(&(log->m_lock));
    pthread_cond_destroy(&(log->m_read_allowed_cv));
    pthread_cond_destroy(&(log->m_write_allowed_cv));

    free(log);
}

void add_to_log(server_log log, const char* data, int data_len) {
    writer_lock(log);

    if (log->debug_sleep_seconds > 0) {      // ✅ ADD THIS (right after writer_lock)
        sleep(log->debug_sleep_seconds);
    }

    //now we are sure no other writer will write to the log
    if(log->m_can_use < data_len){ //no space - need to alloc
        int i=1;
        for(; i*INIT_SIZE < data_len; i++);
        log->m_buffer = realloc(log->m_buffer, (i*INIT_SIZE + log->m_used + log->m_can_use));
        if(log->m_buffer != NULL){
            memcpy(log->m_buffer + log->m_used, data, data_len);
            log->m_used += data_len;
            log->m_can_use += (i*INIT_SIZE - data_len);
        }
    }
    else{ // there is enough space
        memcpy(log->m_buffer + log->m_used, data, data_len);
        log->m_used += data_len;
        log->m_can_use -= data_len;
    }
    writer_unlock(log);
}
/*
int get_log(server_log log, char** dst) {
    reader_lock(log);
    *(dst) = (char*) malloc(log->m_used+1);
    if(*dst != NULL){
        memcpy(*dst, log->m_buffer, log->m_used);
        (*dst)[log->m_used] = '\0';
    }
    int size = log->m_used;
    reader_unlock(log);
    return size;
}
*/

int get_log(server_log log, char **dst)
{
    reader_lock(log);
    if (log->debug_sleep_seconds > 0) {
        sleep(log->debug_sleep_seconds);
    }
    int used = log->m_used;

    if (used >= 2 && log->m_buffer[used - 1] == '\n' && log->m_buffer[used - 2] == '\n') {
        --used; 
    }

    *dst = (char *)malloc(used + 1);
    if (*dst != NULL)
    {
        memcpy(*dst, log->m_buffer, used); 
        (*dst)[used] = '\0';               
    }

    reader_unlock(log);
    return used; 
}
 


