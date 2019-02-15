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


FILE *fp ;
static int _INIT;
static char* LOG_FILE = NULL;

/* print the logs */
void log_print(char* filename,
               int line,
               int log_level,
               char *fmt,...)
{

    va_list args;
    struct timespec tms;
    double _millisec_since_epch;
    char _cwd[PATH_MAX];

    /* output this message only if the
     * logging level is >= of the global logging lvl
    */
    if (log_level >= LOG_OUTPUT_LVL){

        if(_INIT > 0){
          fp = fopen (LOG_FILE,"a+");
        } else {

          /* get the log file name */
          LOG_FILE = getenv("LOG_FILE");
          if (LOG_FILE == NULL){

              /* there is no LOG_FILE, try cwd.. */
              if (getcwd(_cwd, sizeof(_cwd)) == NULL) {

                  perror("getcwd() error");
                  exit(1);

              } else {

                  LOG_FILE = strcat(_cwd, "/logfile.log");

              }
          }

          /* open the file for writing */
          fp = fopen(LOG_FILE,"w");

        }

        /* timestamp */
        if (clock_gettime(CLOCK_REALTIME, &tms)) {
            _millisec_since_epch = -1;
        } else {
            _millisec_since_epch = tms.tv_sec + (double)tms.tv_nsec/1000000.0;
        }

        /* ancillary info and main text */
        fprintf(fp,"[%f][%s][%s][line:%d]: ",
                _millisec_since_epch,
                LOG_LVL_STRING(log_level),
                filename,
                line);

        /* values */
        va_start(args, fmt);
        vfprintf(fp, fmt, args);
        va_end(args);

        /* if the message is ERROR or FATAL */
        if (log_level >= LOG_LVL_ERRO) {
            fprintf(fp, "\n%s\n", strerror(errno));
            fclose(fp);

            if (log_level == LOG_LVL_FATL) {
                exit(1);
            }
        }

        /* add an endline just in case.. */
        fprintf(fp,"\n");

        _INIT++;

        fclose(fp);

    } /* if(log_level>=LOG_OUTPUT_LVL) */
}
