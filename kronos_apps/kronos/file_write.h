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


#ifndef kronos_file_write_H
#define kronos_file_write_H

/* ------------------------------------------------------------------------------------------------------------------ */

#include "kronos/kernels.h"
#include "common/json.h"
#include "common/bool.h"

typedef struct FileWriteConfig {

    long kilobytes;
    long writes;
    long files;
    bool mmap;
    bool o_direct;
    bool continue_files;

    /* Files to write to (absolute filenames). Null if we use the global files. */
    char** file_list;

} FileWriteConfig;

KernelFunctor* init_file_write(const JSON* config_json);


/* ------------------------------------------------------------------------------------------------------------------ */

/* Internal functionality. Exposed for testing purposes */

typedef struct FileWriteParamsInternal {

    long write_size;
    long num_files;
    long first_file_index;
    long num_writes;

} FileWriteParamsInternal;


FileWriteParamsInternal get_write_params(const FileWriteConfig* config);

int get_file_write_name(char* path_out, size_t max_len);

void close_global_write_files();

#endif /* kronos_file_write_H */
