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

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "kronos/cpu.h"
#include "kronos/file_read.h"
#include "kronos/file_write.h"
#include "kronos/fs_metadata.h"
#include "kronos/global_config.h"
#include "kronos/kernels.h"
#include "kronos/mpi_kernel.h"
#include "kronos/memory.h"
#include "kronos/trace.h"

/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * Look over the kernels, and execute each kernel in the list
 */
int execute_kernel_list(const KernelFunctor* kernels) {

    int err, ret = 0, count;

    TRACE2("Iterating %d kernels", kernels->nkernels);

    count = 1;
    while (kernels) {

        TRACE2("Executing kernel %d", count++);

        if ((err = kernels->execute(kernels->data)) != 0)
            ret = err;
        kernels = kernels->next;
    }
    TRACE1("Kernel list iteration complete");
    return ret;
}


KernelFunctor* kernel_factory(const JSON* config) {

    const JSON* name;
    const char* str_name;
    int name_length;

    TRACE();

    name = json_object_get(config, "name");
    if (name == NULL) {
        fprintf(stderr, "Kernel name not found in config\n");
        return NULL;
    }

    name_length = json_string_length(name);
    if (json_as_string_ptr(name, &str_name) != 0) {
        fprintf(stderr, "Kernel name not specified validly\n");
        return NULL;
    }

    /*
     * Select the kernel from the known kernels (if translated to C++, this should be
     * self-registering)
     */
    if (name_length == 9 && strncmp(str_name, "file-read", name_length) == 0) {
        return init_file_read(config);
    } else if (name_length == 10 && strncmp(str_name, "file-write", name_length) == 0) {
        return init_file_write(config);
    } else if (name_length == 11 && strncmp(str_name, "fs_metadata", name_length) == 0) {
        return init_fsmetadata(config);
    } else if (name_length == 3 && strncmp(str_name, "cpu", name_length) == 0) {
        return init_cpu(config);
    } else if (name_length == 6 && strncmp(str_name, "memory", name_length) == 0) {
        return init_memory_kernel(config);
    } else if (name_length == 3 && strncmp(str_name, "mpi", name_length) == 0) {
#ifdef HAVE_MPI
        return init_mpi(config);
#else
        fprintf(stderr, "Synthetic application not built with MPI support");
#endif
    }

    fprintf(stderr, "Kernel \"%*s\" not found\n", name_length, str_name);
    return NULL;
}


KernelFunctor* kernel_list_factory(const JSON* config) {

    KernelFunctor *head, *current, *kernel;
    int i, nkernels;

    TRACE1("Building kernel list ...");

    if (!json_is_array(config)) {
        fprintf(stderr, "Kernel list factory requires a JSON list\n");
        return NULL;
    }

    head = NULL;
    current = NULL;

    nkernels = json_array_length(config);
    for (i = 0; i < nkernels; i++) {

        kernel = kernel_factory(json_array_element(config, i));

        if (kernel == NULL) {
            free_kernel_list(head);
            return NULL;
        }

        kernel->next = NULL;
        kernel->nkernels = nkernels - i;
        if (current != NULL) {
            current->next = kernel;
            current = kernel;
        } else {
            head = kernel;
            current = kernel;
        }
    }

    TRACE1("... done building kernel list");

    return head;
}


void free_kernel_list(KernelFunctor* kernels) {

    KernelFunctor* current_kernel;
    KernelFunctor* next_kernel;

    current_kernel = kernels;
    while (current_kernel) {
        next_kernel = current_kernel->next;
        free_kernel(current_kernel);
        current_kernel = next_kernel;
    }
}


void free_kernel(KernelFunctor* kernel) {

    assert(kernel != NULL);
    assert(kernel->data != NULL);

    free(kernel->data);
    free(kernel);
}


/* ------------------------------------------------------------------------------------------------------------------ */

static long work_accumulator = 0;


long global_distribute_work(long nelems) {

    const GlobalConfig* global_conf = global_config_instance();
    long accumulator_new, nelems_local;

    assert(nelems > 0);
    assert(global_conf->nprocs > 0);

    /* We start from the floor value, and if appropriate, we add one */
    nelems_local = nelems / global_conf->nprocs;

    accumulator_new = (work_accumulator + nelems) % global_conf->nprocs;

    if (accumulator_new > work_accumulator) {
        if (global_conf->mpi_rank >= work_accumulator && global_conf->mpi_rank < accumulator_new)
            nelems_local++;
    } else if (accumulator_new < work_accumulator) {
        if (global_conf->mpi_rank >= work_accumulator || global_conf->mpi_rank < accumulator_new)
            nelems_local++;
    }

    work_accumulator = accumulator_new;

    return nelems_local;
}


void reset_global_distribute() {
    work_accumulator = 0;
}


/* ------------------------------------------------------------------------------------------------------------------ */
