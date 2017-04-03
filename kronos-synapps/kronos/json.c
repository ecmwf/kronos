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
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "kronos/json.h"


static JSON* new_json();
static JSON* new_jsons(int count);
static void swap_jsons(JSON* lhs, JSON* rhs);

typedef struct JSONInput {
    FILE* file;
    const char** string;
} JSONInput;

static JSON* parse_json_internal(JSONInput* input);


/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * Utility routines for manipulating characters read from files
 */

static int peek(JSONInput* input, bool spaces) {

    /*
     * The C standard library doesn't contain a 'peek' function!
     * ==> Get, and unget a character, to implement it
     */

    int c;
    if (input->file) {
        do {
            c = fgetc(input->file);
        } while (c != EOF && !spaces && isspace(c));

        if (c != EOF)
            ungetc(c, input->file);
    } else {
        assert(input->string);
        while ((c = **input->string) != '\0' && !spaces && isspace(**input->string)) {
            (*input->string)++;
        }
    }

    return c;
}


/*
 * A slight upgrade on the getc function
 * --> It skips whitespace (when requested).
 */
static int next(JSONInput* input, bool spaces) {

    int c;
    if (input->file) {
        while ((c = fgetc(input->file)) != EOF) {
            if (spaces || !isspace(c))
                break;
        }
    } else {
        assert(input->string);
        while ((c = **input->string) != '\0') {
            (*input->string)++;
            if (spaces || !isspace(c))
                break;
        }
        if (c == '\0') c = EOF;
    }

    return c;
}

/*
 * Consume the specified character from the file pointer. If the character doesn't match
 * then this is an error
 */
static int consume_char(JSONInput* input, char c) {

    int n = next(input, false);
    if (c != n) {
        fprintf(stderr, "Character '%c' expected and not observed\n", c);
        return -1;
    }
    return 0;
}

/*
 * Consume the specified characters from the file pointer. If the string doesn't match
 * then this is an error
 */
static int consume(JSONInput* input, const char* str) {

    int ret = 0;
    while (*str)
        if ((ret = consume_char(input, *str++)) < 0) break;
    return ret;
}


/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * Actually do some parsing
 */

int parse_true(JSONInput* input, JSON* json) {
    assert(json->type == JSON_NULL);
    json->type = JSON_BOOLEAN;
    json->boolean = true;
    return consume(input, "true");
}

int parse_false(JSONInput* input, JSON* json) {
    assert(json->type == JSON_NULL);
    json->type = JSON_BOOLEAN;
    json->boolean = false;
    return consume(input, "false");
}

int parse_null(JSONInput* input, JSON* json) {
    assert(json->type == JSON_NULL);
    json->type = JSON_NULL;
    return consume(input, "null");
}

int parse_array(JSONInput* input, JSON* json) {

    int c, ret, count;
    JSON *tail, *tmp_json, *next;

    assert(json->type == JSON_NULL);

    ret = consume_char(input, '[');

    /* Build a singly linked list of elements (in reverse order) to store */
    tail = NULL;
    count = 0;

    if (ret >= 0) {
        c = peek(input, false);
        if (c != ']') {

            while (true) {

                tmp_json = parse_json_internal(input);

                if (tmp_json == NULL) {
                    ret = -1;
                    break;
                } else {
                    count++;
                    tmp_json->next = tail;
                    tail = tmp_json;

                    c = peek(input, false);
                    if (c == ']')
                        break;

                    ret = consume_char(input, ',');
                }
            }
        }
    }

    if (ret >= 0) {
        json->type = JSON_ARRAY;
        json->count = count;
        ret = consume_char(input, ']');

        json->array = new_jsons(count);

        /* Copy the elements into an array, to permit O(1) random indexing. */
        tmp_json = tail;
        for (; count > 0; --count) {
            next = tmp_json->next;
            tmp_json->next = 0;

            swap_jsons(&json->array[count-1], tmp_json);
            free_json(tmp_json);

            tmp_json = next;

        }
    }
    return ret;
}

int parse_string(JSONInput* input, JSON* json) {

    int c;
    int length = 0;
    int size = 8;
    char* str = malloc(size);

    assert(json->type == JSON_NULL);

    consume_char(input, '"');

    while (true) {

        c = next(input, true);

        if (c == '"')
            break;

        /* Deal with special characters */
        if (c == '\\') {
            c = next(input, true);
            switch (c) {

            /* Simply append the escaped character */
            case '"': break;
            case '\\': break;
            case '/': break;

            /* Some special characters */
            case 'b': c = '\b'; break;
            case 'f': c = '\f'; break;
            case 'n': c = '\n'; break;
            case 'r': c = '\r'; break;
            case 't': c = '\t'; break;

            /* Unicode \uXXXX format is not supported */
            case 'u': c = -1; break;

            /* Unrecognised escaped character */
            default: c = -1; break;
            }
        }

        /* Catch error conditions (e.g. EOF) */
        if (c < 0) {
            fprintf(stderr, "Error reading string.\n");
            free(str);
            return c;
        }

        /* Otherwise, append the character, extending the string if necessary */
        if (length >= size - 1) {
            size *= 2;
            str = realloc(str, size);
        }
        str[length++] = (char)c;
    }

    /* Ensure that the string is null-terminated */
    assert(size > length);
    str[length] = '\0';

    json->count = length+1; /* Include the null terminator */
    json->string = str;
    json->type = JSON_STRING;

    return 0;
}


int parse_number(JSONInput* input, JSON* json) {

    bool negative = false;
    long negative_exponent = false;

    long integral_part = 0;
    long fractional_part = 0;
    long divisor = 1;
    long exponent = 0;

    int c;

    c = next(input, true);
    if (c == '-') {
        negative = true;
        c = next(input, true);
    } else if (c == '+') {
        c = next(input, true);
    }
    
    if (!isdigit(c)) {
        fprintf(stderr, "Invalid character found parsing number\n");
        return -1;
    }

    integral_part *= 10;
    integral_part += (c - '0');
    while (isdigit(peek(input, true))) {
        c = next(input, true);
        integral_part *= 10;
        integral_part += (c - '0');
    }

    c = peek(input, true);
    if (c == '.') {
        next(input, true);
        while (isdigit((c = peek(input, true)))) {
            c = next(input, true);
            divisor *= 10;
            fractional_part *= 10;
            fractional_part += (c - '0');
        }
    }

    if (negative) {
        integral_part = -integral_part;
        fractional_part = -fractional_part;
    }

    if (c == 'e' || c == 'E') {
        next(input, true);

        c = peek(input, true);
        if (c == '-') {
            negative_exponent = true;
            next(input, true);
        } else if (c == '+') {
            next(input, true);
        }

        while (isdigit(peek(input, true))) {
            c = next(input, true);
            exponent *= 10;
            exponent += (c - '0');
        }

        if (negative_exponent) exponent = -exponent;
    }

    json->type = JSON_NUMBER;
    json->number = integral_part;
    json->number += (double)fractional_part / (double)divisor;
    if (exponent != 0)
        json->number *= pow(10.0, exponent);
    return 0;
}

int parse_object(JSONInput* input, JSON* json) {

    JSON *str_json, *value_json, *current, *next_json;
    bool valid = true;
    int c;

    assert(json->type == JSON_NULL);
    assert(json->count == 0);
    assert(json->array == NULL);

    consume_char(input, '{');

    c = peek(input, false);
    while (c != '}') {

        if (c < 0) {
            fprintf(stderr, "Unexpected character parsing object");
            valid = false;
            break;
        }

        str_json = new_json();
        if (parse_string(input, str_json) != 0) {
            fprintf(stderr, "Error in key in json object\n");
            free_json(str_json);
            valid = false;
            break;
        }
        assert(str_json);
        assert(json_is_string(str_json));

        if (consume_char(input, ':') != 0) {
            fprintf(stderr, "Improperly formed json object\n");
            free_json(str_json);
            valid = false;
            break;
        }

        value_json = parse_json_internal(input);
        if (value_json == NULL) {
            fprintf(stderr, "Error parsing value in json object\n");
            free_json(str_json);
            valid = false;
            break;
        }
        assert(value_json);

        value_json->name = str_json->string;
        value_json->next = json->array;
        json->array = value_json;
        free(str_json); /* n.b. not json_free, as we have discombobulated it */
        json->count++;

        c = peek(input, false);

        if (c == ',') {
            next(input, false);
        } else if (c != '}') {
            fprintf(stderr, "Improperly formed json object\n");
            valid = false;
            break;
        }
    }

    if (valid) {
        consume_char(input, '}');
        json->type = JSON_OBJECT;
    } else {
        current = json->array;
        while (current) {
            next_json = current->next;
            free_json(current);
            current = next_json;
        }
    }

    return valid ? 0 : -1;
}


static JSON* parse_json_internal(JSONInput* input) {

    /* Initialise a new JSON */
    JSON* json = new_json();
    int error, c;

    c = peek(input, false);
    switch(c) {

    case 't': error = parse_true(input, json); break;
    case 'f': error = parse_false(input, json); break;
    case 'n': error = parse_null(input, json); break;
    case '{': error = parse_object(input, json); break;
    case '[': error = parse_array(input, json); break;
    case '\"':error = parse_string(input, json); break;

    case '-': /* Multiple fall-through for various ways to start a number */
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
        error = parse_number(input, json);
        break;

    default:
        fprintf(stderr, "Unexpected character found when parsing JSON\n");
        error = -1;
        break;
    }

    if (error != 0) {
        free_json(json);
        json = NULL;
    }

    return json;
}


JSON* parse_json(FILE* fp) {
    JSONInput input;
    input.file = fp;
    input.string = NULL;
    return parse_json_internal(&input);
}

JSON* json_from_string(const char* str) {
    const char* str_internal = str;
    JSONInput input;
    input.file = NULL;
    input.string = &str_internal;
    return parse_json_internal(&input);
}


/*
 * An allocator and constructor
 */
JSON* new_jsons(int count) {

    int i;
    JSON* json = malloc(count * sizeof(JSON));
    for (i = 0; i < count; i++) {
        json[i].count = 0;
        json[i].name = NULL;
        json[i].string = NULL;
        json[i].array = NULL;
        json[i].next = NULL;
        json[i].type = JSON_NULL;
    }
    return json;
}


JSON* new_json() {

    return new_jsons(1);
}


const JSON* null_json() {

    static JSON json;

    json.type = JSON_NULL;
    json.next = NULL;
    json.array = NULL;
    json.string = NULL;
    json.name = NULL;
    json.count = 0;

    return &json;
}

JSON* json_null_new() {

    JSON* json = new_json();
    return json;
}


JSON* json_string_new_len(const char* str, int len) {

    JSON* json = new_json();

    json->type = JSON_STRING;
    json->count = len+1;
    json->string = malloc(len+1);

    strncpy(json->string, str, len);
    json->string[len] = '\0';

    return json;
}


JSON* json_string_new(const char* str) {

    return json_string_new_len(str, strlen(str));
}

JSON* json_number_new(double val) {

    JSON* json = new_json();

    json->type = JSON_NUMBER;
    json->number = val;
    return json;
}


JSON* json_array_new() {

    JSON* json = new_json();

    json->type = JSON_ARRAY;
}


void json_array_append(JSON* json, JSON* elem) {

    JSON* old_array;
    int i;

    /* Check that either we have no elements, or the array is allocated! */
    assert((json->count == 0) != (json->array != 0));

    old_array = json->array;
    json->array = new_jsons(json->count + 1);

    for (i = 0; i < json->count; i++) {
        swap_jsons(&json->array[i], &old_array[i]);
        destruct_json(&old_array[i]);
    }
    free(old_array);

    swap_jsons(&json->array[json->count], elem);
    free_json(elem);

    json->count++;
}


JSON* json_object_new() {

    JSON* json = new_json();
    json->type = JSON_OBJECT;
}

void json_object_insert(JSON* json, const char* key, JSON* value) {

    /* The key is stored inside the value */

    int len = strlen(key);
    assert(value->name == 0);
    value->name = malloc(len+1);
    strncpy(value->name, key, len+1);

    /* Insert the json into the chain. */

    assert(value->next == 0);
    value->next = json->array;
    json->array = value;

    json->count++;
}


void destruct_json(JSON* json) {

    int i;
    JSON *elem, *next_elem;
    switch (json->type) {

    case JSON_ARRAY:
        assert(json->array != NULL);
        for (i = 0; i < json->count; i++) {
            destruct_json(&json->array[i]);
        }

        free(json->array);
        break;

    case JSON_STRING:
        assert(json->string != NULL);
        free(json->string);
        break;

    case JSON_OBJECT:
        if (json->count > 0) {
            assert(json->array);
            elem = json->array;
            while (elem) {
                next_elem = elem->next;
                free_json(elem);
                elem = next_elem;
            }
        }

    default:
        break;
    }

    if (json->name)
        free(json->name);
}

void free_json(JSON* json) {

    destruct_json(json);

    /* And finally, free the primary structure. */
    free(json);
}


void swap_jsons(JSON* lhs, JSON* rhs) {

    JSON tmp;

    tmp = *rhs;
    *rhs = *lhs;
    *lhs = tmp;
}


/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * Accessors and type testing
 */

bool json_is_null(const JSON* json) {
    assert(json);
    return json->type == JSON_NULL;
}


bool json_is_boolean(const JSON* json) {
    assert(json);
    return json->type == JSON_BOOLEAN;
}


bool json_is_number(const JSON* json) {
    assert(json);
    return json->type == JSON_NUMBER;
}


bool json_is_string(const JSON* json) {
    assert(json);
    return json->type == JSON_STRING;
}


bool json_is_object(const JSON* json) {
    assert(json);
    return json->type == JSON_OBJECT;
}


bool json_is_array(const JSON* json) {
    assert(json);
    return json->type == JSON_ARRAY;
}


int json_as_boolean(const JSON* json, bool* val) {
    assert(json);
    if (json_is_boolean(json)) {
        *val = json->boolean;
        return 0;
    }
    return -1;
}


int json_as_integer(const JSON* json, long* val) {
    assert(json);
    if (json_is_number(json)) {
        *val = (long)json->number;
        return 0;
    }
    return -1;
}


int json_as_double(const JSON* json, double* val) {
    assert(json);
    if (json_is_number(json)) {
        *val = json->number;
        return 0;
    }
    return -1;
}


int json_as_string(const JSON* json, char* val, int max_len) {
    assert(json);
    if (json_is_string(json)) {
        assert(json->string);
        assert(json->count >= 0);
        strncpy(val, json->string, max_len < json->count ? max_len : json->count);
        return 0;
    }
    return -1;
}


int json_as_string_ptr(const JSON* json, const char** val) {
    assert(json);
    if (json_is_string(json)) {
        assert(json->string);
        assert(json->count >= 0);
        *val = json->string;
        return 0;
    }
    return -1;
}


int json_string_length(const JSON* json) {
    assert(json);
    if (json_is_string(json)) {
        assert(json->count >= 0);
        assert(json->string);
        return json->count-1;
    } else {
        return -1;
    }
}


char* json_string_strdup(const JSON* json) {
    assert(json);
    if (json_is_string(json)) {
        assert(json->count >= 0);
        assert(json->string);
        return strdup(json->string);
    } else {
        return NULL;
    }
}

int json_array_length(const JSON* json) {
    assert(json);
    if (json_is_array(json)) {
        assert(json->count >= 0);
        assert(json->count == 0 || json->array != 0);
        return json->count;
    } else {
        return -1;
    }
}

const JSON* json_array_element(const JSON* json, int i) {
    assert(json);
    assert(i >= 0);
    if (json_is_array(json)) {
        if (i < json->count) {
            return json->array + i;
        } else {
            return NULL;
        }
    } else {
        return NULL;
    }
}

int json_object_count(const JSON* json) {
    assert(json);
    if (json_is_object(json)) {
        return json->count;
    } else {
        return -1;
    }
}

bool json_object_has(const JSON* json, const char* key) {
    assert(json);
    assert(key);
    if (json_is_object(json)) {
        const JSON* elem = json->array;
        while (elem) {
            assert(elem->name);
            if (strcmp(key, elem->name) == 0)
                return true;
            elem = elem->next;
        }
    }
    return false;
}

const JSON* json_object_get(const JSON* json, const char* key) {
    assert(json);
    assert(key);
    if (json_is_object(json)) {
        const JSON* elem = json->array;
        while (elem) {
            assert(elem->name);
            if (strcmp(key, elem->name) == 0)
                return elem;
            elem = elem->next;
        }
    }
    return NULL;
}

int json_object_get_integer(const JSON* json, const char* key, long* value) {

    const JSON* elem;
    int error = -1;

    assert(json);
    assert(key);

    elem = json_object_get(json, key);
    if (elem) {
        if (json_as_integer(elem, value) == 0) {
            error = 0;
        } else {
            fprintf(stderr, "Error reading field \"%s\" as integer\n", key);
        }
    } else {
        fprintf(stderr, "Field \"%s\" not found\n", key);
    }

    return error;
}

int json_object_get_boolean(const JSON* json, const char* key, bool* value) {

    const JSON* elem;
    int error = -1;

    assert(json);
    assert(key);

    elem = json_object_get(json, key);
    if (elem) {
        if (json_as_boolean(elem, value) == 0) {
            error = 0;
        } else {
            fprintf(stderr, "Error reading field \"%s\" as boolean\n", key);
        }
    } else {
        fprintf(stderr, "Field \"%s\" not found\n", key);
    }

    return error;
}


/* ------------------------------------------------------------------------------------------------------------------ */

/*
 * Routines for printing jsons
 */

static void print_json_array(FILE* fp_out, const JSON* json) {

    int i;
    bool first = true;

    assert(json->type == JSON_ARRAY);

    fprintf(fp_out, "[");
    for (i = 0; i < json->count; i++) {
        fprintf(fp_out, (first ? "": ","));
        write_json(fp_out, &json->array[i]);
        first = false;
    }
    fprintf(fp_out, "]");
}

static void print_json_string(FILE* fp_out, const JSON* json) {

    int i;

    assert(json->type == JSON_STRING);
    assert(json->string != NULL);
    assert(json->count >= 0);
    assert(json->string[json->count-1] == '\0');

    putc('"', fp_out);

    for (i = 0; i < json->count-1; i++) {
        switch (json->string[i]) {
        case '"':
        case '\\':
        case '/':
            putc('\\', fp_out);
            putc(json->string[i], fp_out);
            break;
        case '\b':
            fprintf(fp_out, "\\b");
            break;
        case '\f':
            fprintf(fp_out, "\\f");
            break;
        case '\n':
            fprintf(fp_out, "\\n");
            break;
        case '\r':
            fprintf(fp_out, "\\r");
            break;
        case '\t':
            fprintf(fp_out, "\\t");
            break;
        default:
            putc(json->string[i], fp_out);
        }
    }

    putc('"', fp_out);

    /*
     * n.b. the trivial implementation doesn't escape anything
     *
     * fprintf(fp_out, "\"%.*s\"", json->count, json->string);
     */
}


static void print_json_number(FILE* fp_out, const JSON* json) {

    assert(json->type == JSON_NUMBER);

    if (fabs(((double)(long)json->number) - json->number) == 0.0) {
        fprintf(fp_out, "%li", (long)json->number);
    } else {
        fprintf(fp_out, "%e", json->number);
    }
}


static void print_json_object(FILE* fp_out, const JSON* json) {

    bool first = true;
    const JSON* elem = json->array;

    assert(json->type == JSON_OBJECT);

    fprintf(fp_out, "{");
    while (elem) {

        if (!first)
            fprintf(fp_out, ",");

        assert(elem->name);
        fprintf(fp_out, "\"%s\":", elem->name);
        write_json(fp_out, elem);

        first = false;
        elem = elem->next;
    }

    fprintf(fp_out, "}");

}

void write_json(FILE* fp_out, const JSON* json) {

    switch (json->type) {

    case JSON_NULL:    fprintf(fp_out, "null"); break;
    case JSON_BOOLEAN: fprintf(fp_out, (json->boolean ? "true" : "false")); break;
    case JSON_NUMBER:  print_json_number(fp_out, json); break;
    case JSON_STRING:  print_json_string(fp_out, json); break;
    case JSON_OBJECT:  print_json_object(fp_out, json); break;
    case JSON_ARRAY:   print_json_array(fp_out, json); break;

    default:
        fprintf(fp_out, "Unknown component");
        break;
    }
}

void print_json(FILE* fp_out, const JSON* json) {

    fprintf(fp_out, "JSON(");
    write_json(fp_out, json);
    fprintf(fp_out, ")");
}
