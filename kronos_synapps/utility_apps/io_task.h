

#define MAX_WRITE_SIZE 1048576


/* an IO task (defines a file read or write operation)*/
typedef struct IOTask {

    const char* type;
    const char* file;
    const char* write_mode;

    long int n_bytes;
    long int offset;
    long int n_writes;

} IOTask;


/* get IO task type [writer/reader]*/
const char* get_iotask_type(IOTask* iotask);

/* get IO task filename */
const char* get_iotask_file(IOTask* iotask);

/* get IO task mode [append|create] - for writer only */
const char* get_iotask_mode(IOTask* iotask);

/* get IO task n bytes to write/read */
long int get_iotask_bytes(IOTask* iotask);

/* get IO task n of write/read */
long int get_iotask_nwrites(IOTask* iotask);

/* get IO task file offset */
long int get_iotask_offset(IOTask* iotask);


/* parse a json strign and populate an IO task */
void iotask_fromstring(IOTask* iotask, const char* _taskstr);
