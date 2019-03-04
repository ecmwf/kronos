#ifndef NVRAM_LAYOUT_H
#define NVRAM_LAYOUT_H

#ifdef HAVE_PMEMIO

    #define LAYOUT_NAME "worker_obj"
    /*#define MAX_PMEM_BUF_LEN 1048576*/
    #define MAX_PMEM_BUF_LEN 444
    
    struct kronos_pobj_root {
        char buf[MAX_PMEM_BUF_LEN];
    };
    
    #endif

#endif
