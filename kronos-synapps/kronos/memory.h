#ifndef MEMORY_H
#define MEMORY_H

/* ------------------------------------------------------------------------------------------------------------------ */
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>
#include "kronos/kernels.h"
#include "kronos/json.h"

typedef struct MEMConfig {

    long mem_kb;

} MEMConfig;

KernelFunctor* init_mem(const JSON* config_json);

/* ------------------------------------------------------------------------------------------------------------------ */

/* Internal functionality. Exposed for testing purposes */

typedef struct MEMParamsInternal {

    long node_mem;

} MEMParamsInternal;


MEMParamsInternal get_MEM_params(const MEMConfig* config);

/* ------------------------------------------------------------------------------------------------------------------ */


#endif
