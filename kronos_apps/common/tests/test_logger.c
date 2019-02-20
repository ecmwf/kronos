
#include "common/logger.h"


/* We want to ensure that assert is always defined */
#ifdef NDEBUG
#undef NDEBUG
#include <assert.h>
#define NDEBUG
#else
#include <assert.h>
#endif

#include "stdio.h"
#include <string.h>
#include <stdlib.h>
#include <limits.h>

#include "common/logger.h"


#define PATH_MAX_FORTESTS 160

/* some basic tests of the logger */
static void test_logger(){

    char* log_file;
    char cwd[PATH_MAX_FORTESTS];
    char cwd_and_filepath[PATH_MAX_FORTESTS];
    char* LOG_FILE;

    putenv("LOG_FILE=test_logger.tmp");
    log_file = getenv("LOG_FILE");

    if (getcwd(cwd, sizeof(cwd)) != '\n') {

        strcpy(cwd_and_filepath, cwd);
        LOG_FILE = strcat(cwd_and_filepath, "/logfile.log");

    } else {
        perror("getcwd() error");
        exit(1);
    }

    /* simply check that the file name is correct */
    assert(!strcmp(LOG_FILE, cwd_and_filepath));


    /* put some env variables */
    putenv("LOG_STREAM_STD=1");
    putenv("LOG_STREAM_ERR=1");
    putenv("LOG_STREAM_FIL=0");
    putenv("LOG_OUTPUT_LVL=1");

    /* test the init condition */
    assert(get_start_flag() == 0);
    INFO1("1st msg to start the logging");
    assert(get_start_flag() == 1);
    INFO1("2nd msg: flag should be 1.");
    assert(get_start_flag() == 1);
    INFO1("3rd msg: flag should be 1.");
    assert(get_start_flag() == 1);

    /* other user settings */
    assert(get_std_flag() == 1);
    assert(get_err_flag() == 1);
    assert(get_fil_flag() == 0);
    assert(get_out_lvl() == 1);

    /* printf("cwd %s\n", cwd);
    printf("LOG_FILE %s\n", LOG_FILE);
    printf("cwd_and_filepath %s\n", cwd_and_filepath); */


}


int main() {

    test_logger();

    return 0;
}
