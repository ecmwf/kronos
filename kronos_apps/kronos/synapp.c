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

#include "kronos/file_write.h"
#include "kronos/frames.h"
#include "kronos/global_config.h"
#include "common/json.h"
#include "kronos/stats.h"
#include "kronos/notification.h"


/* ------------------------------------------------------------------------------------------------------------------ */

int main(int argc, char** argv)
{
    int err = -1;
    FILE* fp;
    JSON* json;
    const JSON* frames_json;
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

            /* We want to log the time series behaviour of the kernels, not the
             * ancilliary bumpf. So only log the kernels */
            start_time_series_logging();

            if (frames == NULL) {
                fprintf(stderr, "Error initialising kernel list\n");
            } else {
                err = execute_frame_list(frames);
                free_frame_list(frames);
            }

            /* Statistics reporting on what has happened */

            report_stats();
            free_stats_registry();
            close_global_write_files();
            free_json(json);
            send_final_notification();
            clean_global_config();
        } else {
            fprintf(stderr, "Error parsing json...\n");
        }

        fclose(fp);
    } else {
        fprintf(stderr, "Failed to open input file %s\n", input_filename);
    }

    return err;
}
