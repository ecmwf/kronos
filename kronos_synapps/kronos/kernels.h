/*
 * (C) Copyright 1996-2015 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/**
 * @date May 2016
 * @author Simon Smart
 */


#ifndef kronos_kernels_H
#define kronos_kernels_H

#include "kronos/json.h"

/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * Each kernel has different configuration. To store each of them in a generic list, they contain a generic funciton
 * pointer and data pointer
 */

typedef struct KernelFunctor {

    struct KernelFunctor* next;

    int (*execute)(const void* data);

    void* data;

    /* Contains the number of kernels remaining in the list (inclusive of this one) if known */
    int nkernels;

} KernelFunctor;


KernelFunctor* kernel_factory(const JSON* config);
KernelFunctor* kernel_list_factory(const JSON* config);
void free_kernel_list(KernelFunctor* kernels);
void free_kernel(KernelFunctor* kernel);

int execute_kernel_list(const KernelFunctor* kernels);


/*
 * If we have n elements of work (e.g. file reads) distribute them over the processors in a
 * systematic and global way, rather than each kernel doing its own thing. This prevents
 * the same nodes ending up with more reads and more writes, for example.
 */
long global_distribute_work(long nelems);

/* For use in the test suite */
void reset_global_distribute();

/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_kernels_H */
