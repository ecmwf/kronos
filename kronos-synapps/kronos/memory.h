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

/* Longer name than normal, to avoid ambiguity that we are initting memory */
KernelFunctor* init_memory_kernel(const JSON* config_json);

/* ------------------------------------------------------------------------------------------------------------------ */

/* Internal functionality. Exposed for testing purposes */

typedef struct MEMParamsInternal {

    long node_mem;

} MEMParamsInternal;


MEMParamsInternal get_MEM_params(const MEMConfig* config);

/* ------------------------------------------------------------------------------------------------------------------ */


#endif
