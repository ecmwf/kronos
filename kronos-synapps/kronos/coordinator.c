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

#include <stdio.h>
#include <stdlib.h>

#include "kronos/json.h"
#include "kronos/global_config.h"
#include "kronos/frames.h"
#include "kronos/stats.h"


/* ------------------------------------------------------------------------------------------------------------------ */

int main(int argc, char** argv)
{
    int err = -1;
    FILE* fp;
    JSON* json;
    const JSON* frames_json;
    JSON* stats_json;
    const char* input_filename;
    Frame* frames;

    if (argc == 1) {
        input_filename = "input.json";
        printf("Using default input file: %s\n", input_filename);
    } else {
        input_filename = argv[1];
    }

    fp = fopen(input_filename, "r");
    if (fp != NULL) {

        json = parse_json(fp);

        if (json != NULL) {

            frames = NULL;
            if (init_global_config(json, argc, argv) != 0) {
                fprintf(stderr, "Error initialising global config\n");
            } else {

                if ((frames_json = json_object_get(json, "frames")) == NULL) {
                    fprintf(stderr, "Failed to obtain frames from configuration JSON\n");
                } else {
                    frames = frame_list_factory(frames_json);
                }
            }

            if (frames == NULL) {
                fprintf(stderr, "Error initialising kernel list\n");
            } else {
                err = execute_frame_list(frames);
                free_frame_list(frames);
            }

            /* Statistics reporting on what has happened */
            report_stats(stderr);
            stats_json = report_stats_json();
            print_json(stderr, stats_json);
            free_json(stats_json);
            free_stats_registry();

            clean_global_config();
            free_json(json);
        } else {
            fprintf(stderr, "Error parsing json...\n");
        }

        fclose(fp);
    } else {
        fprintf(stderr, "Failed to open input file %s\n", input_filename);
    }

    return err;
}
