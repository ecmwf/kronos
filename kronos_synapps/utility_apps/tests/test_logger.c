
/* We want to ensure that assert is always defined */
#ifdef NDEBUG
#undef NDEBUG
#include <assert.h>
#define NDEBUG
#else
#include <assert.h>
#endif



#include "utility_apps/logger.h"


static void test_logger(){

}


int main() {

    test_logger();

    return 0;
}
