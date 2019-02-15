#ifndef MEMORY_PERSIST_H
#define MEMORY_PERSIST_H

#ifdef HAVE_PMEMIO

/* ------------------------------------------------------------------------------------------------------------------ */

#include <unistd.h>
#include <string.h>
#include <sys/mman.h>

#include "kronos/kernels.h"
#include "kronos/json.h"

typedef struct MEMPersistConfig {

    long mem_kb;

} MEMPersistConfig;

/* Longer name than normal, to avoid ambiguity that we are initting memory */
KernelFunctor* init_mem_persist_kernel(const JSON* config_json);

/* ------------------------------------------------------------------------------------------------------------------ */

/* Internal functionality. Exposed for testing purposes */

typedef struct MEMPersistParamsInternal {

    long node_mem;

} MEMPersistParamsInternal;


MEMPersistParamsInternal get_MEMPersist_params(const MEMPersistConfig* config);

/* ------------------------------------------------------------------------------------------------------------------ */

# endif /* HAVE_PMEMIO */

#endif
