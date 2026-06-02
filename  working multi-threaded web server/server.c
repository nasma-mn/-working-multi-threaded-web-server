#include "segel.h"
#include "request.h"
#include "log.h"

int THREADS;
int QUEUE_SIZE;
int DEBUG1 = 7;
int debug_sleep_time;

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//


struct request {
    int             m_fd;
    struct timeval  m_arrival, m_dispatch;
    int m_requestNum;
    threads_stats   m_thread;          // pointer to the thread handling it
} typedef request;


request* buildAndInitRequest(int fd){
    request *req = malloc(sizeof(request));
    //they said wa can assume all allocations success
    if(req==NULL){

    }
    req->m_fd = fd;
    gettimeofday(&(req->m_arrival), NULL);
    return req;
}

void destroyRequest(request* req){
    free(req);
    req = NULL;
}

//we'll save the requests in a queue
//so we built a queue as seen in the tut.8
struct reqQueue{
    int m_size;
    int m_fictiveSize;
    int m_head, m_tail;
    request** m_buffer;
    pthread_cond_t m_not_full_cv;
    pthread_cond_t m_not_empty_cv;
    pthread_cond_t m_main_can_add_cv;
    pthread_mutex_t m_lock;
}typedef reqQueue;

// Create the global server log
static server_log requestsLog;
static reqQueue queue;
static pthread_key_t stats_key;


void initReqQueue(reqQueue* q, int size){
    q->m_size = 0; // no elements yet
    q->m_fictiveSize = 0;
    q->m_head = 0; //as the head of the queue and it's tail points to buffer[0]
    q->m_tail = 0;
    q->m_buffer = malloc(size*sizeof(char*));
    pthread_cond_init(&(q->m_not_full_cv), NULL);
    pthread_cond_init(&(q->m_not_empty_cv), NULL);
    pthread_cond_init(&(q->m_main_can_add_cv), NULL);
    pthread_mutex_init(&(q->m_lock), NULL);
}

void destroyReqQueue(reqQueue* q){
    free(q->m_buffer);
    q->m_buffer = NULL;
    pthread_cond_destroy(&(q->m_not_full_cv));
    pthread_cond_destroy(&(q->m_not_empty_cv));
    pthread_cond_destroy(&(q->m_main_can_add_cv));
    pthread_mutex_destroy(&(q->m_lock));
}

void enqueueReq(reqQueue* q, request* req){
    pthread_mutex_lock(&(q->m_lock));

    // add x to tail because we need FIFO queue 
    while(q->m_fictiveSize == QUEUE_SIZE){
        pthread_cond_wait(&(q->m_not_full_cv), &(q->m_lock));
    }
    q->m_buffer[(q->m_tail) % QUEUE_SIZE] = req;
    q->m_tail = (q->m_tail +1) % QUEUE_SIZE;
    q->m_size++;
    q->m_fictiveSize++;

    pthread_cond_signal(&(q->m_not_empty_cv));
    pthread_mutex_unlock(&(q->m_lock));
}

request* popReq(reqQueue* q){
    pthread_mutex_lock(&(q->m_lock));

    /* remove x from head because we need FIFO queue */
    while(q->m_size == 0){
        pthread_cond_wait(&(q->m_not_empty_cv), &(q->m_lock));
    }

    q->m_size--;
    // we'll not distract the request here, since we need to return it
    request* to_return =  queue.m_buffer[(q->m_head) % QUEUE_SIZE];
    q->m_head = (q->m_head +1) % QUEUE_SIZE;

    //we'll give signal that the queue is not full just when the fictive size is reduced, since we can't 
    //get more than QUEUE_SIZE requests including those being handled now
    //pthread_cond_signal(&(q->m_not_full_cv));
    pthread_mutex_unlock(&(q->m_lock));

    return to_return;
}

void reduceFictiveSize(reqQueue* q){
    pthread_mutex_lock(&(q->m_lock));

    q->m_fictiveSize -=1;

    pthread_cond_signal(&(q->m_main_can_add_cv));
    pthread_cond_signal(&(q->m_not_full_cv));
    pthread_mutex_unlock(&(q->m_lock));
    return;
}

// Parses command-line arguments
void getargs(int *port, int argc, char *argv[])
{
    

    *port = atoi(argv[1]);
    THREADS = atoi(argv[2]);
    QUEUE_SIZE = atoi (argv[3]);
    debug_sleep_time = atoi(argv[4]);
}


// TODO: HW3 — Initialize thread pool and request queue
// This server currently handles all requests in the main thread.
// You must implement a thread pool (fixed number of worker threads)
// that process requests from a synchronized queue.

void printForDebug(int x){
    //printf("%d", x);
    x++;
}

static void *f(void *arg){
    int id = *(int *)arg;
    printForDebug(id);
    threads_stats s = calloc(1, sizeof(struct Threads_stats));
    s->id = id;

    pthread_setspecific(stats_key, s);

    // each thread should handle the requests one after another!
    while(1){
        int x=0;
        request *req = popReq(&queue);

        gettimeofday(&(req->m_dispatch), NULL);


        time_stats ts;
        
        threads_stats t = (threads_stats)pthread_getspecific(stats_key);

	    //timersub(&(req->m_dispatch), &req->m_arrival, &(req->m_dispatch));

        req->m_thread = pthread_getspecific(stats_key);  

        ts.task_arrival = req->m_arrival;
        ts.task_dispatch = req->m_dispatch;
        // Call the request handler in the proper thread
        requestHandle(req->m_fd, ts, t, requestsLog);

        Close(req->m_fd); // Close the connection
        destroyRequest(req);
	reduceFictiveSize(&queue);
    }
    return NULL;
}

// a linked list to save the ids allocated and then frees them



int main(int argc, char *argv[])
{
    if (argc != 5) {
        fprintf(stderr,
                "Usage: %s <port> <threads> <queue_size> <debug_sleep_time>\n",
                argv[0]);
        exit(1);
    }

    for(int i=0; i<DEBUG1; i++){
        //printf("a");
        //printf("\n");
    }

    int listenfd, connfd, port, clientlen;
    struct sockaddr_in clientaddr;


    pthread_key_create(&stats_key, free);

    getargs(&port, argc, argv);              // ✅ parse debug_sleep_time here


   if (THREADS <= 0 || QUEUE_SIZE <= 0 || debug_sleep_time < 0 ||
    port < 1024 || port > 65535) {
    fprintf(stderr, "Invalid arguments\n");
    exit(1);
    }


    requestsLog = create_log();              // ✅ now create log
    set_log_debug_sleep(requestsLog, debug_sleep_time);   // ✅ ADD THIS LINE

    initReqQueue(&queue, QUEUE_SIZE);        // ✅ now QUEUE_SIZE is valid too

    // TODO: HW3 — Add cleanup code for thread pool and queue

    //as seen in tut.7

    //since we can not have more than QUEUE_SIZE requests including those being handled now, there is no need to create more threads than the QUEUE_SIZE
    // so if QUEUE_SIZE<THREADS, we'll make threadsNum=QUEUE_SIZE, not THREADS.

    pthread_t threads[THREADS];
    int* idArrayToFree[THREADS];
    for (unsigned int i=0; i<THREADS; i++){
        int* id = malloc(1*sizeof(int));
        *id = i+1; //so we start the threads numbering from 1
        pthread_create(&threads[i], NULL, f, id);
        pthread_detach(threads[i]); // we're not gonna use the finishing value of the thread or ask about it
        // this way we avoid memory leaks from unjoined threads
        //add the id to the array to free it later;
	idArrayToFree[i] = id;
        printForDebug(i);
    }

    for(int i=0; i<DEBUG1; i++);

    listenfd = Open_listenfd(port);
    while (1) {
	pthread_mutex_lock(&(queue.m_lock));
        while(queue.m_fictiveSize == QUEUE_SIZE){
	    pthread_cond_wait(&(queue.m_main_can_add_cv), &(queue.m_lock));
        }
	pthread_mutex_unlock(&(queue.m_lock));

        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t *) &clientlen);
        request* req = buildAndInitRequest(connfd);
        enqueueReq(&queue, req);
    }


    // Clean up the server log before exiting
    for (unsigned int i=0; i<THREADS; i++){
	free(idArrayToFree[i]);
    }
    destroy_log(requestsLog);
    destroyReqQueue(&queue);
    return 0;
}
