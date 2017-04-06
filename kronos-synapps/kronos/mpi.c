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

#include <stdlib.h>
#include <math.h>

#include "kronos/mpi.h"
#include "kronos/global_config.h"
#include "kronos/trace.h"
#include "kronos/utility.h"
#include "kronos/stats.h"

/* ------------------------------------------------------------------------------------------------------------------ */

#ifdef HAVE_MPI

/* ------------------------------------------------------------------------------------------------------------------ */

static StatisticsLogger* collective_stats_instance() {

    static StatisticsLogger* logger = 0;

    if (logger == 0)
        logger = create_stats_times_bytes_logger("mpi-collective");

    return logger;
}


static StatisticsLogger* pairwise_stats_instance() {

    static StatisticsLogger* logger = 0;

    if (logger == 0)
        logger = create_stats_times_bytes_logger("mpi-pairwise");

    return logger;
}


MPIParamsInternal get_mpi_params(const MPIConfig* config) {

    MPIParamsInternal params;

    /*
     * The kb_collective measure specifies the average kb per node, so this value does NOT need
     * to be divided by the number of nodes.
     *
     * To start with, use the simplest possible MPI collective operation - the broadcast.
     */

    if (config->ncollective == 0) {
        params.nbroadcast = 0;
        params.broadcast_size = 0;
    } else {
        params.nbroadcast = config->ncollective;
        params.broadcast_size = 1024 * config->kb_collective / config->ncollective;
    }

    /*
     * The pairwise actions are decided during the execute stage, according to a triangular
     * mapping
     */

    if (config->npairwise == 0) {
        params.npairwise = 0;
        params.pairwise_size = 0;
    } else {
        params.npairwise = config->npairwise;
        params.pairwise_size = 1024 * config->kb_pairwise / config->npairwise;
    }

    return params;
}


static int execute_mpi(const void* data) {

    const GlobalConfig* global_conf = global_config_instance();
    const MPIConfig* config = data;
    MPIParamsInternal params;

    /* The FNV prime (a 64-bit prime number) */
    const unsigned long p2p_offset = 0x100000001b3;
    const int pairwise_tag = 0xbeef;
    unsigned long npairs, pair_index, node_index;
    int node, nodea, nodeb;
    MPI_Status status;

    char* buffer;

    long i;
    int err;

    params = get_mpi_params(config);

    /*
     * To ensure uniform coverage, we step through the nodes at a rate determined by a large prime (modulo the number
     * of nodes), and use this as the source node for the broadcast.
     */

    TRACE3("Performing %li MPI_Broadcast operations, each of %li bytes", params.nbroadcast, params.broadcast_size);

    if (params.nbroadcast != 0) {

        buffer = malloc(params.broadcast_size);

        node_index = 0;
        for (i = 0; i < params.nbroadcast; i++) {

            node_index = (node_index + p2p_offset) % global_conf->nprocs;
            node = node_index;

            TRACE3("Performing broadcast of %d bytes from node %d", params.broadcast_size, node);
            stats_start(collective_stats_instance());
            err = MPI_Bcast(buffer, params.broadcast_size, MPI_CHAR, node, MPI_COMM_WORLD);
            stats_stop_log_bytes(collective_stats_instance(), params.broadcast_size);
        }

        free(buffer);
    }


    /* For a triangular mapping:
     *
     * 01
     * 02 12
     * 03 13 23
     * ...
     *
     * Given a zero based index X, we can find the processor indices A, B using:
     *
     * B = ceiling((sqrt(9 + 8X) - 1) / 2)
     * A = X - B * (B -1) / 2
     *
     * To ensure uniform coverage, we step through the list (modulo the largest available
     * triangular number) by a reasonably large prime nummber.
     */

    TRACE3("Performing %li MPI_Send/Recv operations, each of %li bytes", params.npairwise, params.pairwise_size);

    if (params.npairwise != 0) {

        buffer = malloc(params.pairwise_size);

        npairs = global_conf->nprocs * (global_conf->nprocs - 1) / 2;
        pair_index = 0;

        for (i = 0; i < params.npairwise; i++) {

            pair_index = (pair_index + p2p_offset) % npairs;
            nodeb = (int)ceil((sqrt((double)(9 + 8 * pair_index)) - 1) / 2);
            nodea = pair_index - (nodeb * (nodeb - 1) / 2);

            TRACE4("MPI sending %d bytes from node %d to %d", params.pairwise_size, nodea, nodeb);
            stats_start(pairwise_stats_instance());
            if (global_conf->mpi_rank == nodea) {
                err = MPI_Send(buffer, params.pairwise_size, MPI_CHAR, nodeb, pairwise_tag, MPI_COMM_WORLD);
            } else if (global_conf->mpi_rank == nodeb) {
                err = MPI_Recv(buffer, params.pairwise_size, MPI_CHAR, nodea, pairwise_tag, MPI_COMM_WORLD, &status);
            }
            stats_stop_log_bytes(pairwise_stats_instance(), params.pairwise_size);

        }

        free(buffer);
    }

    /* Ignore MPI errors */
    (void) err;

    return 0;
}


KernelFunctor* init_mpi(const JSON* config_json) {

    MPIConfig* config;
    KernelFunctor* functor;

    const GlobalConfig* global_conf = global_config_instance();

    TRACE();

    config = malloc(sizeof(MPIConfig));

    if (json_object_get_integer(config_json, "kb_collective", &config->kb_collective) != 0 ||
        json_object_get_integer(config_json, "n_collective", &config->ncollective) != 0 ||
        json_object_get_integer(config_json, "kb_pairwise", &config->kb_pairwise) != 0 ||
        json_object_get_integer(config_json, "n_pairwise", &config->npairwise) != 0 ||
        config->kb_collective < 0 ||
        config->ncollective < 0 ||
        config->kb_pairwise < 0 ||
        config->npairwise < 0 ||
        (config->npairwise == 0 && config->kb_pairwise != 0) ||
        (config->ncollective == 0 && config->kb_collective != 0)) {

        fprintf(stderr, "Invalid parameters specified in mpi config\n");
        free(config);
        return NULL;
    }

    if (config->npairwise != 0 && global_conf->nprocs < 2) {
        fprintf(stderr, "Point-to-point MPI operations requested with fewer than 2 MPI nodes\n");
        free(config);
        return NULL;
    }

    functor = malloc(sizeof(KernelFunctor));
    functor->next = NULL;
    functor->execute = &execute_mpi;
    functor->data = config;

    return functor;
}

/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* HAVE_MPI */
