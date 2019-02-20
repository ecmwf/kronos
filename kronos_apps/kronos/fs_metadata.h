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
 * @date June 2016
 * @author Simon Smart
 */


#ifndef kronos_fs_metadata_H
#define kronos_fs_metadata_H

/* ------------------------------------------------------------------------------------------------------------------ */ 

#include "kronos/kernels.h"
#include "common/json.h"

typedef struct FsMetadataConfig {

    long n_mkdirs;

} FsMetadataConfig;

KernelFunctor* init_fsmetadata(const JSON* config_json);

/* ------------------------------------------------------------------------------------------------------------------ */

/* Internal functionality. Exposed for testing purposes */

typedef struct FsMetadataParamsInternal {

    long node_n_mkdirs;

} FsMetadataParamsInternal;


FsMetadataParamsInternal get_fsmetadata_params(const FsMetadataConfig* config);

/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_cpu_H */
