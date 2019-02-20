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

#include "kronos/frames.h"
#include "kronos/trace.h"

/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * Loop over the frames, and execute the lists of events
 */
int execute_frame_list(const Frame* frames) {

    int err, ret = 0, frame;

    TRACE2("Iterating frame list of %d frames", frames->nframes);

    frame = 1;
    while (frames) {

        TRACE2("Starting frame %d", frame++);

        if ((err = execute_kernel_list(frames->kernels)) != 0)
            ret = err;
        frames = frames->next;
    }
    TRACE1("Frame list iteration complete");
    return ret;
}


Frame* frame_factory(const JSON* config) {

    KernelFunctor *kernels;
    Frame *frame;

    TRACE();

    kernels = kernel_list_factory(config);

    if (kernels) {

        frame = malloc(sizeof(Frame));
        frame->kernels = kernels;

        return frame;
    } else
        return NULL;
}


Frame* frame_list_factory(const JSON* config) {

    Frame *head, *current, *frame;
    int i, nframes;

    TRACE1("Building frame list ...");

    if (!json_is_array(config)) {
        fprintf(stderr, "Frame list factory requires a JSON list\n");
        return NULL;
    }

    head = NULL;
    current = NULL;

    nframes = json_array_length(config);
    for (i = 0; i < nframes; i++) {

        frame = frame_factory(json_array_element(config, i));

        if (frame == NULL) {
            free_frame_list(head);
            return NULL;
        }

        frame->next = NULL;
        frame->nframes = nframes - i;
        if (current != NULL) {
            current->next = frame;
            current = frame;
        } else {
            head = frame;
            current = frame;
        }
    }

    TRACE1("... done building frame list");

    return head;
}


void free_frame_list(Frame* frames) {

    Frame* current_frame;
    Frame* next_frame;

    current_frame = frames;
    while (current_frame) {
        next_frame = current_frame->next;
        free_kernel_list(current_frame->kernels);
        free(current_frame);
        current_frame = next_frame;
    }
}


/* ------------------------------------------------------------------------------------------------------------------ */
