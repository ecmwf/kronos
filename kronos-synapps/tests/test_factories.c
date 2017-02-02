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

/* We want to ensure that assert is always defined */
#ifdef NDEBUG
#undef NDEBUG
#include <assert.h>
#define NDEBUG
#else
#include <assert.h>
#endif
#include <stdlib.h>

#include "kronos/frames.h"
#include "kronos/file_read.h"

/* ------------------------------------------------------------------------------------------------------------------ */

static void test_invalid_kernel_specified() {

    /* Name not specified */

    JSON* json = json_from_string("{\"other\": \"invalid\"}");
    assert(json);
    assert(kernel_factory(json) == NULL);
    free_json(json);

    /* Name is not a string... */

    json = json_from_string("{\"name\": 1234}");
    assert(json);
    assert(kernel_factory(json) == NULL);
    free_json(json);

    /* Name not in list of avialable kernels */

    json = json_from_string("{\"name\": \"invalid\"}");
    assert(json);
    assert(kernel_factory(json) == NULL);
    free_json(json);
}


static void test_kernel_list() {

    KernelFunctor* kernels;

    /* What happens if we pass simply _wrong_ JSONs to the list factory? */

    JSON* json = json_from_string("{\"other\": \"invalid\"}");
    assert(json);
    assert(kernel_list_factory(json) == NULL);
    free_json(json);

    /* An empty list */

    json = json_from_string("[]");
    assert(json);
    assert(kernel_list_factory(json) == NULL);
    free_json(json);

    /* A list of kernels with an error in one of the kernels should unwind the list correctly */
    json = json_from_string("[{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": 3},{\"name\": \"file-read\", \"kb_read\": \"invalid\", \"n_read\": 3}]");
    assert(json);
    assert(kernel_list_factory(json) == NULL);
    free_json(json);

    /* A list of kernels */

    json = json_from_string("[{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": 3},{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": 3}]");
    assert(json);
    kernels = kernel_list_factory(json);
    assert(kernels != NULL);

    assert(kernels->nkernels == 2);
    assert(kernels->data != NULL);
    assert(((const FileReadConfig*)kernels->data)->reads == 3);
    assert(kernels->next != NULL);
    assert(kernels->next->nkernels == 1);
    assert(kernels->next->data != NULL);
    assert(((const FileReadConfig*)kernels->next->data)->reads == 3);
    assert(kernels->next->next == NULL);

    free_kernel_list(kernels);
    free_json(json);
}


static void test_frame_list() {

    Frame* frames;

    /* What happens if we pass simply _wrong_ JSONs to the list factory? */

    JSON* json = json_from_string("{\"other\": \"invalid\"}");
    assert(json);
    assert(frame_list_factory(json) == NULL);
    free_json(json);

    /* Need kernel lists, not anything else, inside the outer frames list */

    json = json_from_string("[{}]");
    assert(json);
    assert(frame_list_factory(json) == NULL);
    free_json(json);

    json = json_from_string("[[], {}]");
    assert(json);
    assert(frame_list_factory(json) == NULL);
    free_json(json);

    /* An empty frame list, or empty kernels? */

    json = json_from_string("[]");
    assert(json);
    assert(frame_list_factory(json) == NULL);
    free_json(json);

    json = json_from_string("[[], []]");
    assert(json);
    assert(frame_list_factory(json) == NULL);
    free_json(json);

    /* If there is an error in one of the contained kernels? */
    json = json_from_string("[[{\"name\": \"file-read\", \"kb_read\": \"invalid\", \"n_read\": 3}]]");
    assert(json);
    assert(frame_list_factory(json) == NULL);
    free_json(json);

    /* Full fledged generation of kernel lists!!! */
    json = json_from_string("[[{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": 3},{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": 3}],[{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": 3},{\"name\": \"file-read\", \"kb_read\": 1234, \"n_read\": 3}]]");
    assert(json);
    frames = frame_list_factory(json);
    assert(frames != NULL);

    assert(frames->nframes == 2);

    assert(frames->kernels->nkernels == 2);
    assert(frames->kernels->data != NULL);
    assert(((const FileReadConfig*)frames->kernels->data)->reads == 3);
    assert(frames->kernels->next != NULL);
    assert(frames->kernels->next->nkernels == 1);
    assert(frames->kernels->next->data != NULL);
    assert(((const FileReadConfig*)frames->kernels->next->data)->reads == 3);
    assert(frames->kernels->next->next == NULL);

    assert(frames->next != NULL);
    assert(frames->next->nframes == 1);
    assert(frames->next->next == NULL);

    assert(frames->next->kernels->nkernels == 2);
    assert(frames->next->kernels->data != NULL);
    assert(((const FileReadConfig*)frames->next->kernels->data)->reads == 3);
    assert(frames->next->kernels->next != NULL);
    assert(frames->next->kernels->next->nkernels == 1);
    assert(frames->next->kernels->next->data != NULL);
    assert(((const FileReadConfig*)frames->next->kernels->next->data)->reads == 3);
    assert(frames->next->kernels->next->next == NULL);

    free_frame_list(frames);
    free_json(json);

}


/* ------------------------------------------------------------------------------------------------------------------ */

int main() {

    test_invalid_kernel_specified();
    test_kernel_list();
    test_frame_list();
    return 0;
}
