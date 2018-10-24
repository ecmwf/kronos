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


#ifndef kronos_file_read_H
#define kronos_file_read_H

/* ------------------------------------------------------------------------------------------------------------------ */

#include "kronos/kernels.h"
#include "kronos/json.h"
#include "kronos/bool.h"

typedef struct FileReadConfig {

    long kilobytes;
    long reads;
    bool mmap;
    bool o_direct;
    bool invalidate;

    /* Files to read from (absolute filenames). Null if we use the global files. */

    long nfiles;
    char** file_list;

} FileReadConfig;

KernelFunctor* init_file_read(const JSON* config_json);


/* ------------------------------------------------------------------------------------------------------------------ */

/* Internal functionality. Exposed for testing purposes */

typedef struct FileReadParamsInternal {

    long read_size;

    long num_reads;

    /* Only used with file list as above */

    long* file_sizes;

} FileReadParamsInternal;


FileReadParamsInternal get_read_params(const FileReadConfig* config);


/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_file_read_H */
