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

/** Only for comparison purposes. Not an allocator */
const JSON* null_json();

JSON* parse_json(FILE* fp);

JSON* json_from_string(const char* str);

void free_json(JSON* json);

/* Print json is slightly more human readable */
void print_json(FILE* fp_out, const JSON* json);
void write_json(FILE* fp_out, const JSON* json);

/**
 * Write a json into a provided string buffer. Returns the number of characters written including
 * the terminal \0, or the number of characters required if larger than the
 * specified size
 */
int write_json_string(char* str, int size, const JSON* json);

/*
 * Constructing jsons
 */

JSON* json_null_new();
JSON* json_string_new(const char* str);
JSON* json_string_new_len(const char* str, int len);
JSON* json_number_new(double val);
JSON* json_array_new();
JSON* json_object_new();

/** Append an element to a json array. n.b. This takes ownership of elem (and frees it) */
void json_array_append(JSON* json, JSON* elem);

/** Insert an element into a json object. n.b. This takes ownership of elem (and frees it) */
void json_object_insert(JSON* json, const char* key, JSON* elem);

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
int json_object_get_double(const JSON* json, const char* key, double* value);


/* ------------------------------------------------------------------------------------------------------------------ */

#endif /* krons_json_H */

