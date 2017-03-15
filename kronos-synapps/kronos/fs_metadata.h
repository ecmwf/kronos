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
#include "kronos/json.h"

typedef struct FSMETAConfig {

    long n_mk_dirs;
    long n_rm_dirs;

} FSMETAConfig;

KernelFunctor* init_fsmetadata(const JSON* config_json);

/* ------------------------------------------------------------------------------------------------------------------ */

/* Internal functionality. Exposed for testing purposes */

typedef struct FSMETAParamsInternal {

    long node_n_mk_dirs;
    long node_n_rm_dirs;

} FSMETAParamsInternal;


FSMETAParamsInternal get_fsmetadata_params(const FSMETAConfig* config);

/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_cpu_H */
