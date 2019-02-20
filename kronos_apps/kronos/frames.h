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


#ifndef kronos_frames_H
#define kronos_frames_H

#include "kronos/kernels.h"
#include "common/json.h"

/* ------------------------------------------------------------------------------------------------------------------ */

typedef struct Frame {

    struct Frame* next;

    KernelFunctor* kernels;

    /* Contains the number of frames remaining in the list (inclusive of this one) if known */
    int nframes;

} Frame;


Frame* frame_factory(const JSON* config);
Frame* frame_list_factory(const JSON* config);
void free_frame_list(Frame* frames);

int execute_frame_list(const Frame* frames);

/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* kronos_frames_H */
