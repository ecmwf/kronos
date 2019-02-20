
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
#include <stdlib.h>



/* some basic tests of the logger */
static void test_logger(){


    char* log_file;

    putenv("LOG_FILE=test_logger.tmp");
    log_file = getenv("LOG_FILE");

    assert(log_file == "test_logger.tmp");


}


int main() {

    test_logger();

    return 0;
}
