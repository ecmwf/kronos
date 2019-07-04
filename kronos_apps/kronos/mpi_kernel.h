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


#ifndef kronos_mpi_H
#define kronos_mpi_H

/* Take care to suppress compiler warnings associated with long long (which is not
 * supported in C89 */

#ifdef HAVE_MPI

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wlong-long"
#endif /* __GNUC__ */

#include <mpi.h>

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif /* __GNUC__ */

#endif /* HAVE_MPI */


/* ------------------------------------------------------------------------------------------------------------------ */

#include "kronos/kernels.h"
#include "common/json.h"

typedef struct MPIConfig {

    long ncollective, npairwise;
    long kb_collective, kb_pairwise;

} MPIConfig;

KernelFunctor* init_mpi(const JSON* config_json);

/* ------------------------------------------------------------------------------------------------------------------ */

/* Internal functionality. Exposed for testing purposes */

typedef struct MPIParamsInternal {

    long nbroadcast, broadcast_size;
    long npairwise, pairwise_size;

} MPIParamsInternal;


MPIParamsInternal get_mpi_params(const MPIConfig* config);

/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_mpi_H */
