#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>

#include "logger.h"


/* flags the beginning of the logs */
static unsigned short LOG_STARTED = 0;

/* logging settings from env variables */
static char* LOG_FILE = NULL;
static unsigned int LOG_STREAM_STD = _LOG_STREAM_STD_DEFAULT;
static unsigned int LOG_STREAM_ERR = _LOG_STREAM_ERR_DEFAULT;
static unsigned int LOG_STREAM_FIL = _LOG_STREAM_FIL_DEFAULT;
static unsigned int LOG_OUTPUT_LVL = LOG_LVL_DEBG;


/* get logger env variables (only at the beginning) */
static void get_logger_env(){

    char * _cwd;

    if (getenv("LOG_FILE") != NULL){

        /* log file defined by env variable */
        LOG_FILE = getenv("LOG_FILE");

    } else {  /* try to get a log file in the cwd */

        if (getcwd(_cwd, sizeof(_cwd)) != NULL) {
            LOG_FILE = strcat(_cwd, "/logfile.log");
        } else {
            perror("getcwd() error");
            exit(1);
        }

    }

    if (getenv("LOG_STREAM_STD") != NULL){
        LOG_STREAM_STD = (unsigned int)atoi(getenv("LOG_STREAM_STD"));
    }

    if (getenv("LOG_STREAM_ERR") != NULL){
        LOG_STREAM_ERR = (unsigned int)atoi(getenv("LOG_STREAM_ERR"));
    }

    if (getenv("LOG_STREAM_FIL") != NULL){
        LOG_STREAM_FIL = (unsigned int)atoi(getenv("LOG_STREAM_FIL"));
    }

    if (getenv("LOG_OUTPUT_LVL") != NULL){
        LOG_OUTPUT_LVL = (unsigned int)atoi(getenv("LOG_OUTPUT_LVL"));
    }


    printf("log file: %s\n", LOG_FILE);

}

/* print to stdout */
static void print_to_stdout(const char* log_msg){
    fprintf(stdout,"%s", log_msg);
}


/* print to stderr */
static void print_to_stderr(const char* log_msg){
    fprintf(stdout,"%s", log_msg);
}


/* print to file (write|append) */
static void print_to_file(const char* log_msg){

    FILE *fp;

    fp = fopen(LOG_FILE,"a");
    if (fp != NULL) {
        fprintf(fp,"%s", log_msg);
        fclose(fp);
    }

}


/* print the logs */
void log_print(char* filename,
               int line,
               int log_level,
               char *fmt,...)
{

    va_list args;
    struct timespec tms;
    double _millisec_since_epch;

    /* header-body-time */
    char msg_head[LOG_STR_LEN];
    char msg_body[LOG_STR_LEN];
    char log_msg[LOG_STR_LEN];

    /* get env variables */
    if (!LOG_STARTED){
        get_logger_env();
        LOG_STARTED = 1;
    }

    if (log_level >= LOG_OUTPUT_LVL){

        /* timestamp */
        if (clock_gettime(CLOCK_REALTIME, &tms)) {
            _millisec_since_epch = -1;
        } else {
            _millisec_since_epch = tms.tv_sec + (double)tms.tv_nsec/1000000.0;
        }

        /* header info */
        sprintf(msg_head,"[%f][%s][%s][line:%d]: ",
                _millisec_since_epch,
                LOG_LVL_STRING(log_level),
                filename,
                line);

        /* main body of the log message */
        va_start(args, fmt);
        vsprintf(msg_body, fmt, args);
        va_end(args);

        /* put everything together */
        sprintf(log_msg, "%s: %s\n", msg_head, msg_body);

        /* stream the log to stdout */
        if (LOG_STREAM_STD) {
            print_to_stdout(log_msg);
        }

        /* stream the log to stderr */
        if (LOG_STREAM_ERR) {
            print_to_stderr(log_msg);
        }

        /* stream the log to logfile */
        if (LOG_STREAM_FIL) {
            print_to_file(log_msg);
        }


        /* make it fail if it's a fatal */
        if (log_level == LOG_LVL_FATL) {
            exit(1);
        }

    } /* if(log_level>=LOG_OUTPUT_LVL) */
}
