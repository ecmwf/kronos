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


#ifndef kronos_json_H
#define kronos_json_H


#include <stdio.h>

#include "kronos/bool.h"


/* ------------------------------------------------------------------------------------------------------------------ */

enum json_value_type {
    JSON_NULL,
    JSON_BOOLEAN,
    JSON_NUMBER,
    JSON_STRING,
    JSON_OBJECT,
    JSON_ARRAY
};


typedef struct JSON {

    enum json_value_type type;

    bool boolean;
    int count;
    char* string;
    char* name;
    double number;

    struct JSON* next;
    struct JSON* array;

} JSON;

const JSON* null_json();

JSON* parse_json(FILE* fp);

JSON* json_from_string(const char* str);

void free_json(JSON* json);

/* Print json is slightly more human readable */
void print_json(FILE* fp_out, const JSON* json);
void write_json(FILE* fp_out, const JSON* json);

/*
 * Type testing
 */

bool json_is_null(const JSON* json);
bool json_is_boolean(const JSON* json);
bool json_is_number(const JSON* json);
bool json_is_string(const JSON* json);
bool json_is_object(const JSON* json);
bool json_is_array(const JSON* json);

/*
 * Accessors
 */

int json_as_boolean(const JSON* json, bool* val);
int json_as_integer(const JSON* json, long* val);
int json_as_double(const JSON* json, double* val);
int json_as_string(const JSON* json, char* val, int max_len);
int json_as_string_ptr(const JSON* json, const char** val);

int json_string_length(const JSON* json);
char* json_string_strdup(const JSON* json);

int json_array_length(const JSON* json);
const JSON* json_array_element(const JSON* json, int i);

int json_object_count(const JSON* json);
bool json_object_has(const JSON* json, const char* key);
const JSON* json_object_get(const JSON* json, const char* key);

int json_object_get_integer(const JSON* json, const char* key, long* value);
int json_object_get_boolean(const JSON* json, const char* key, bool* value);


/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* krons_json_H */

